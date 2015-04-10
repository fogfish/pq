%%
%%   Copyright (c) 2012 - 2013, Dmitry Kolesnikov
%%   All Rights Reserved.
%%
%%   Licensed under the Apache License, Version 2.0 (the "License");
%%   you may not use this file except in compliance with the License.
%%   You may obtain a copy of the License at
%%
%%       http://www.apache.org/licenses/LICENSE-2.0
%%
%%   Unless required by applicable law or agreed to in writing, software
%%   distributed under the License is distributed on an "AS IS" BASIS,
%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%   See the License for the specific language governing permissions and
%%   limitations under the License.
%%
%% @description
%%   pq pool leaser - dispatches requests to workers 
-module(pq_pool).
-behaviour(gen_fsm).
-include("pq.hrl").

-export([
   start_link/1 
  ,start_link/2 
   % gen_fsm
  ,init/1
  ,terminate/3
  ,active/2
  ,active/3
  ,inactive/2
  ,inactive/3
  ,handle_info/3
  ,handle_event/3
  ,handle_sync_event/4
  ,code_change/4
   % api
  ,close/1
  ,lease/2
  ,release/2
  ,suspend/1
  ,resume/1
  ,worker/1
  ,ioctl/2
]).

-record(pool, {
   wq       = ?NULL       :: datum:q()  % queue of worker processes
  ,size     = 0           :: integer()  % worker queue size
  ,capacity = 10          :: integer()  % worker queue capacity
  ,type     = reusable    :: disposable | reusable
  ,worker   = undefined   :: any()      % worker specification
  ,ttl      = ?CONFIG_TTL :: integer()
}).

%%%------------------------------------------------------------------
%%%
%%% Factory
%%%
%%%------------------------------------------------------------------

%%
%%
start_link(Opts) ->
   gen_fsm:start_link(?MODULE, [Opts], []).

start_link(Name, Opts) ->
   gen_fsm:start_link({local, Name}, ?MODULE, [Opts], []).

init([Opts]) ->
   {ok, inactive, init(Opts, #pool{})}.

init([{capacity, X} | Opts], State) ->
   init(Opts, State#pool{capacity=X});

init([{type, X} | Opts], State) ->
   init(Opts, State#pool{type=X});

init([{worker, X} | Opts], State) ->
   init(Opts, State#pool{worker=X});

init([{ttl, X} | Opts], State) ->
   init(Opts, State#pool{ttl=X});

init([_ | Opts], State) ->
   init(Opts, State);

init([], State) ->
   erlang:process_flag(trap_exit, true),
   gen_fsm:send_event(self(), resume),
   State.

terminate(_, _Sid, State) ->
   erlang:process_flag(trap_exit, false),
   free_pool(State#pool.wq),
   ok.

%%%------------------------------------------------------------------
%%%
%%% api
%%%
%%%------------------------------------------------------------------

%%
%%
close(Pool) ->
   gen_fsm:send_all_state_event(Pool, close).

%%
%% 
lease(Pool, Opts) ->
   gen_fsm:sync_send_event(Pool, {lease, Opts}, infinity).

%%
%%
release(Pool, Pid) ->
   gen_fsm:send_event(Pool, {release, Pid}).

%%
%%
suspend(Pool) ->
   gen_fsm:send_event(Pool, suspend).

%%
%%
resume(Pool) ->
   gen_fsm:send_event(Pool, resume).

%%
%%
worker(Pool) ->
   gen_fsm:sync_send_all_state_event(Pool, worker, infinity).

%%
%%
ioctl(Pool, Req) ->
   gen_fsm:sync_send_all_state_event(Pool, {ioctl, Req}, infinity).

%%%------------------------------------------------------------------
%%%
%%% gen_fsm
%%%
%%%------------------------------------------------------------------

%%
%%
active({lease, _}, _Tx, #pool{wq=?NULL, size=Size, capacity=Capacity}=State)
 when Size =:= Capacity ->
   {reply, {error, ebusy}, active, State};

active({lease, _}=Req, Tx, #pool{wq=?NULL, size=Size, type=Type, ttl=TTL, worker=Worker}=State) ->
   {ok, Pid} = pq_uow:start_link(self(), Type, TTL, Worker),
   active(Req, Tx, State#pool{wq=q:enq(Pid, ?NULL), size=Size + 1});

active({lease, Opts}, Tx, State) ->
   %% lease UoW process and bind it with client
   ?DEBUG("pq [pool]: ~p lease ~p~n", [self(), Tx]),   
   {Pid, Queue} = q:deq(State#pool.wq),
   pq_uow:lease(Pid, Tx, Opts),
   {next_state, active, State#pool{wq=Queue}};

active(Msg, Tx, State) ->
   ?WARNING("pq [pool]: unexpected message ~p from ~p~n", [Msg, Tx]),
   {next_state, active, State}.

%%
%%
active({release, Pid}, State) ->
   ?DEBUG("pq [pool]: ~p release ~p~n", [self(), Pid]),   
   %% release UoW process to queue
   {next_state, active, 
      State#pool{
         wq = q:enq(Pid, State#pool.wq)
      }
   };

active(suspend, State) ->
   ?DEBUG("pq [pool]: ~p suspend~n", [self()]),   
   {next_state, inactive, 
      State#pool{
         wq = free_pool(State#pool.wq)
      }
   };

active(resume, State) ->
   {next_state, active, State};

active(Msg, State) ->
   ?WARNING("pq [pool]: unexpected message ~p~n", [Msg]),
   {next_state, active, State}.

%%
%%
inactive({lease, _}, _Tx, State) ->
   {reply, {error, ebusy}, inactive, State};

inactive(Msg, Tx, State) ->
   ?WARNING("pq [pool]: unexpected message ~p from ~p~n", [Msg, Tx]),
   {next_state, inactive, State}.

%%
%%
inactive({release, Pid}, State) ->
   ?DEBUG("pq [pool]: ~p release~n", [self(), Pid]),   
   pq_uow:close(Pid),
   {next_state, inactive, State};

inactive(suspend, State) ->
   {next_state, inactive, State};

inactive(resume, State) ->
   ?DEBUG("pq [pool]: ~p resume~n", [self()]),   
   {next_state, active,
      State#pool{
         wq = init_pool(0, State#pool.type, State#pool.ttl, State#pool.worker, q:new())
      }
   };

inactive(Msg, State) ->
   ?WARNING("pq [pool]: unexpected message ~p~n", [Msg]),
   {next_state, inactive, State}.

%%
%%
handle_info({'EXIT', _Old, _Reason}, active, State) ->
   ?DEBUG("pq [pool]: ~p death of ~p due ~p~n", [self(), _Old, _Reason]),   
   {ok, Pid} = pq_uow:start_link(self(), State#pool.type, State#pool.ttl, State#pool.worker),
   {next_state, active,  
      State#pool{
         wq = q:enq(Pid, State#pool.wq)
      }
   };

handle_info({'EXIT', _Old, _Reason}, inactive, State) ->
   ?DEBUG("pq [pool]: ~p death of ~p due ~p~n", [self(), _Old, _Reason]),   
   {next_state, inactive, State};

handle_info(_Msg, Sid, State) ->
   {next_state, Sid, State}.

%%
%%
handle_event(close, _Sid, State) ->
   {stop, normal, State};

handle_event(_Msg, Sid, State) ->
   {next_state, Sid, State}.

%%
%%
handle_sync_event({ioctl, capacity}, _Tx, Sid, #pool{size=N}=State) ->
   {reply, N, Sid, State};

handle_sync_event({ioctl, busy}, _Tx, Sid, #pool{wq=Q, size=N}=State) ->
   {reply, N - q:length(Q), Sid, State};

handle_sync_event({ioctl, free}, _Tx, Sid, #pool{wq=Q}=State) ->
   {reply, q:length(Q), Sid, State};

handle_sync_event(worker, _Tx, Sid, State) ->
   {reply, State#pool.worker, Sid, State};

handle_sync_event(_Msg, _Tx, Sid, State) ->
   {next_state, Sid, State}.

%%
%%
code_change(_OldVsn, Sid, State, _Extra) ->
   {ok, Sid, State}.

%%%------------------------------------------------------------------
%%%
%%% private
%%%
%%%------------------------------------------------------------------

%%
%% initialize pool of UoW processes
init_pool(0, _Type, _TTL, _Worker, Queue) ->
   Queue;
init_pool(N,  Type,  TTL,  Worker, Queue) ->
   {ok, Pid} = pq_uow:start_link(self(), Type, TTL, Worker),
   init_pool(N - 1, Type, TTL, Worker, q:enq(Pid, Queue)).

%%
%% terminate pool of UoW processes
free_pool(?NULL) ->
   ?NULL;
free_pool(Queue) ->
   pq_uow:close(q:head(Queue)),
   free_pool(q:tail(Queue)).
