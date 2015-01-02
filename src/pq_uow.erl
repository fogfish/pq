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
%%   unit of work - container of worker process
-module(pq_uow).
-behaviour(gen_fsm).

-include("pq.hrl").

-export([
   start_link/4
   % gen_fsm
  ,init/1
  ,terminate/3
  ,idle/2
  ,inuse/2
  ,expired/2
  ,handle_info/3
  ,handle_event/3
  ,handle_sync_event/4
  ,code_change/4
  % api
  ,close/1
  ,lease/2
  ,release/1
]).

%% internal state
-record(fsm, {
   pool   = undefined :: pid()       %% pool process
  ,client = undefined :: reference() %% client reference
  ,type   = undefined :: atom()      %% worker re-use strategy
  ,worker = undefined :: any()       %% worker process specification
  ,ttl    = undefined :: any()       %% worker process timeout
  ,pid    = undefined :: pid()       %% worker process instance
}).

%%%------------------------------------------------------------------
%%%
%%% factory
%%%
%%%------------------------------------------------------------------

start_link(Pool, Type, TTL, Worker) ->
   gen_fsm:start_link(?MODULE, [Pool, Type, TTL, Worker], []).

init([Pool, Type, TTL, Worker]) ->
   erlang:process_flag(trap_exit, true),
   {ok, idle,
      #fsm{
         pool   = Pool
        ,type   = Type
        ,ttl    = TTL
        ,worker = Worker
      }
   }.

terminate(_Reason, _Sid, State) ->
   free_worker(State#fsm.pid),
   ok.

%%%------------------------------------------------------------------
%%%
%%% api
%%%
%%%------------------------------------------------------------------

%%
%%
close(Pid) ->
   gen_fsm:send_all_state_event(Pid, close).

%%
%%
lease(Pid, Tx) ->
   gen_fsm:send_event(Pid, {lease, Tx}).

%%
%%
release(#pq{uow=Pid}=Ref) ->
   gen_fsm:send_event(Pid, {release, Ref}).

%%%------------------------------------------------------------------
%%%
%%% gen_fsm
%%%
%%%------------------------------------------------------------------

%%
%%
idle({lease, Tx}=Req, #fsm{pid=undefined}=State) ->
   ?DEBUG("pq [uow]: ~p init worker ~p~n", [State#fsm.pool, State#fsm.worker]),      
   case init_worker(State#fsm.worker) of
      {ok, Pid} ->
         set_ttl(State#fsm.ttl),
         idle(Req, State#fsm{pid=Pid});
      {error, Reason} ->
         gen_server:reply(Tx, {error, Reason}),
         pq_pool:release(State#fsm.pool, self()),
         {next_state, idle, State}
   end;

idle({lease, {Pid, _}=Tx}, State) ->
   Ref = erlang:monitor(process, Pid),
   gen_server:reply(Tx, #pq{uow=self(), pid=State#fsm.pid}),
   {next_state, inuse, 
      State#fsm{
         client = Ref
      }
   };

idle({release, _}, State) ->
   {next_state, idle, State}.

%%
%%
inuse({lease, Tx}, State) ->
   gen_server:reply(Tx, {error, ebusy}),
   {next_state, inuse, State};

inuse({release, _}, #fsm{type=disposable}=State) ->
   ?DEBUG("pq [uow]: ~p free worker ~p~n", [State#fsm.pool, State#fsm.pid]),      
   _ = erlang:demonitor(State#fsm.client, [flush]),
   pq_pool:release(State#fsm.pool, self()),
   _ = free_worker(State#fsm.pid),
   {next_state, idle, State#fsm{pid=undefined}};
   
inuse({release, _}, State) ->
   _ = erlang:demonitor(State#fsm.client, [flush]),
   pq_pool:release(State#fsm.pool, self()),
   {next_state, idle, State}.

%%
%%
expired({release, _}, State) ->
   ?DEBUG("pq [uow]: ~p free worker ~p~n", [State#fsm.pool, State#fsm.pid]),      
   _ = erlang:demonitor(State#fsm.client, [flush]),
   pq_pool:release(State#fsm.pool, self()),
   _ = free_worker(State#fsm.pid),
   {next_state, idle, State#fsm{pid=undefined}};

expired(Msg, State) ->
   inuse(Msg, State).

%%
%%
handle_info({'DOWN', _Ref, _Type, _Pid, _Reason}, inuse, State) ->
   %% client process dies (worker state is nondeterministic)
   _ = free_worker(State#fsm.pid),
   pq_pool:release(State#fsm.pool, self()),
   {next_state, idle, State#fsm{client=undefined, pid=undefined}};

handle_info({'EXIT', Pid, Reason}, idle, State) ->
   ?DEBUG("pq [uow]: ~p death of worker ~p due ~p~n", [State#fsm.pool, Pid, Reason]),      
   {next_state, idle, State#fsm{pid=undefined}};

handle_info({'EXIT', Pid, Reason}, inuse, State) ->
   ?DEBUG("pq [uow]: ~p death of worker ~p due ~p~n", [State#fsm.pool, Pid, Reason]),      
   pq_pool:release(State#fsm.pool, self()),
   {next_state, idle, State#fsm{pid=undefined}};

handle_info(ttl, idle, State) ->
   ?DEBUG("pq [uow]: ~p expired ~p~n", [State#fsm.pool, State#fsm.pid]),      
   _ = free_worker(State#fsm.pid),
   {next_state, idle, State#fsm{pid=undefined}};

handle_info(ttl, inuse, State) ->
   ?DEBUG("pq [uow]: ~p expired ~p~n", [State#fsm.pool, State#fsm.pid]),      
   {next_state, expired, State};


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
%%
set_ttl(infinity) ->
   ok;
set_ttl(TTL)  ->
   erlang:send_after(TTL, self(), ttl).


%%
%%
init_worker({Mod, Fun, Args}) ->
   erlang:apply(Mod, Fun, Args);
init_worker({Mod, Args}) ->
   erlang:apply(Mod, start_link, Args);
init_worker(Mod) ->
   Mod:start_link().

%%
%%
free_worker(undefined) ->
   ok;
free_worker(Pid) ->
   Ref = erlang:monitor(process, Pid),
   unlink(Pid),
   receive
      {'EXIT', Pid, _Reason} ->
         erlang:demonitor(Ref, [flush]),
         ok
   after 0 ->
      exit(Pid, shutdown),
      receive 
         {'DOWN', Ref, _Type, Pid, _Reason} ->
            ok
      after 5000 ->
         exit(Pid, kill),  
         receive 
            {'DOWN', Ref, _Type, Pid, _Reason} ->
               ok
         end
      end
   end.







