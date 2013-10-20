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
%%   pq leader allocates requests among workers 
-module(pq_leader).
-behaviour(gen_server).
-include("pq.hrl").

-export([
   start_link/4, 
   % gen_server
   init/1, 
   terminate/2,
   handle_call/3, 
   handle_cast/2, 
   handle_info/2,  
   code_change/3
]).

-record(leader, {
   type             :: disposable | reusable,
   factory          :: pid(),     % worker factory
   linger   = inf   :: integer(), % max number of delayed requests   
   capacity = 0     :: integer(), % worker queue capacity (remaining workers)
   size     = 0     :: integer(), % worker queue size     (max number of workers)
   inactive = false :: boolean(), % queue is suspended
   lq,              % lease queue
   wq,              % worker queue
   ondemand = false,              % flag to indicates one-demand worker allocation
   worker   = undefined           % opaque worker specification (used as global dictionary)
}).

%%%------------------------------------------------------------------
%%%
%%% Factory
%%%
%%%------------------------------------------------------------------

%%
%%
start_link(Sup, undefined, Opts, Worker) ->
   gen_server:start_link(?MODULE, [Sup, Opts, Worker], []);

start_link(Sup, Name, Opts, Worker) ->
   gen_server:start_link({local, Name}, ?MODULE, [Sup, Opts, Worker], []).

init([Sup, Opts, Worker]) ->
   self() ! {resolve_factory, Sup},
   {ok, init(Opts, #leader{worker=Worker})}.

init([{capacity, X} | Opts], S)
 when is_integer(X) ->
   init(Opts, S#leader{capacity=X, size=X});

init([{linger,   X} | Opts], S)
 when is_integer(X) ->
   init(Opts, S#leader{linger=X});

init([{type, X} | Opts], S) ->
   init(Opts, S#leader{type=X});

init([ondemand | T], S) ->
   init(T, S#leader{ondemand=true});

init([_ | Opts], S) ->
   init(Opts, S);

init([], S) ->
   S#leader{
      lq = deq:new(),
      wq = deq:new()
   }.

terminate(_, _) ->
   ok.

%%%------------------------------------------------------------------
%%%
%%% gen_server
%%%
%%%------------------------------------------------------------------

%%
%%
handle_call({lease, Timeout}, Tx, #leader{capacity=0}=S) ->
   % no capacity - wait for spare one
   {noreply, enq_request(Timeout, Tx, S)};

handle_call({lease, Timeout}, Tx, #leader{inactive=true}=S) ->
   % queue is not active
   {noreply, enq_request(Timeout, Tx, S)};

handle_call({lease, Timeout}, Tx, S) ->
   {noreply, deq_worker(enq_request(Timeout, Tx, S))};

%%
%%
handle_call({release, _Pid}, Tx, #leader{inactive=true}=S) ->
   % TODO: shutdown Pid
   gen_server:reply(Tx, ok), % reply immediately
   {noreply, S};

handle_call({release, _Pid}, Tx, #leader{type=disposable}=S) ->
   % no needs to kill worker - it kill itself
   % no needs to increase capacity, death of worker made it
   % we have to wait for worker termination so that 
   % it is capable to perform graceful exit
   gen_server:reply(Tx, ok),
   {noreply, S};

handle_call({release, Pid}, Tx, #leader{type=reusable}=S) ->
   % no needs to kill worker - it is reusable
   gen_server:reply(Tx, ok),
   case is_process_alive(Pid) of
      true  -> 
         {noreply, deq_worker(enq_worker(Pid, S))};
      % no needs to increase capacity, death of worker made it   
      false ->
         {noreply, S}
   end;

%%
%%
handle_call(suspend, _, S) ->
   ?DEBUG("pq ~p suspend", [self()]),
   free_worker(S#leader.wq),
   {reply, ok, 
      S#leader{
         inactive = true,
         capacity = S#leader.size,
         wq       = deq:new()
      }
   };

handle_call(resume, _, #leader{inactive=false}=S) ->
   {reply, ok, S};

handle_call(resume, _, #leader{ondemand=false}=S) ->
   ?DEBUG("pq ~p resume", [self()]),
   Workers = [init_worker(S#leader.factory) || _ <- lists:seq(1, S#leader.capacity)],
   {reply, ok,
      deq_worker(
         S#leader{
            inactive = false,
            wq       = lists:foldl(fun deq:enq/2, deq:new(), Workers)
         }
      )
   };

handle_call(resume, _, S) ->
   {reply, ok, 
      deq_worker(
         S#leader{
            inactive = false
         }
      )
   };

%%
%%
handle_call(worker, _, S) ->
   {reply, {ok, S#leader.worker}, S};

handle_call(_, _, S) ->
   {noreply, S}.

%%
%%
handle_cast(_, S) ->
   {noreply, S}.

%%
%%
handle_info({resolve_factory, Sup}, #leader{ondemand=false}=S) ->
   {_, Pid, _, _} = lists:keyfind(pq_worker_sup, 1, supervisor:which_children(Sup)),
   Workers = [init_worker(Pid) || _ <- lists:seq(1, S#leader.capacity)],
   {noreply, 
      S#leader{
         factory = Pid,
         wq      = lists:foldl(fun deq:enq/2, deq:new(), Workers)
      }
   };

handle_info({resolve_factory, Sup}, S) ->
   {_, Pid, _, _} = lists:keyfind(pq_worker_sup, 1, supervisor:which_children(Sup)),
   {noreply, S#leader{factory=Pid}};

handle_info({'DOWN', _, _, _Pid, _Reason}, #leader{inactive=true}=S) ->
   % queue is not active, do not recover worker
   {noreply, S};

handle_info({'DOWN', _, _, Pid, Reason}, #leader{ondemand=false}=S) ->
   % loss of worker is (release empty), we have to decrease capacity
   % to ensure that new worker is restarted  
   ?DEBUG("pq ~p die worker: ~p, reason ~p", [self(), Pid, Reason]),
   {noreply, 
      deq_worker(
         enq_worker(
            dec_capacity(S)
         )
      )
   };

handle_info({'DOWN', _, _, Pid, Reason}, S) ->
   ?DEBUG("pq ~p die worker: ~p, reason ~p", [self(), Pid, Reason]),
   {noreply, 
      deq_worker(
         inc_capacity(S)
      )
   };

handle_info(_, S) ->
   {noreply, S}.

%%
%% 
code_change(_Vsn, S, _Extra) ->
   {ok, S}.


%%%------------------------------------------------------------------
%%%
%%% private
%%%
%%%------------------------------------------------------------------

%%
%% enqueue lease request
enq_request(Timeout, Req, S) ->
   ?DEBUG("pq ~p (c=~b) enq request: ~p (timeout ~p)", [self(), S#leader.capacity, Req, Timeout]),
   % clean-up queue
   Q = deq:dropwhile(fun pq_util:expired/1, S#leader.lq),
   case deq:length(Q) of
      X when X =< S#leader.linger ->
         S#leader{lq = deq:enq({lease, pq_util:deadline(Timeout), Req}, Q)};
      _ ->
         gen_server:reply(Req, {error, no_capacity}),
         S#leader{lq = Q}
   end.

%%
%% enqueue worker
enq_worker(#leader{capacity=C, size=Size}=S)
 when C =< Size ->
   % limit number of workers
   enq_worker(init_worker(S#leader.factory), S);

enq_worker(S) ->
   S.

enq_worker(Worker, S) ->
   ?DEBUG("pq ~p (c=~b) enq worker: ~p", [self(), S#leader.capacity, Worker]),
   inc_capacity(
      S#leader{
         wq = deq:enq(Worker, S#leader.wq)
      }
   ).

%%
%%
deq_worker(#leader{lq={}}=S) ->
   % no valid lease request
   ?DEBUG("pq ~p (c=~b) deq worker: no lease", [self(), S#leader.capacity]),
   S;

deq_worker(#leader{wq={}}=S) ->
   % no valid worker
   Worker = init_worker(S#leader.factory),
   {{lease, _, Req}, Lq} = deq:deq(S#leader.lq), 
   ?DEBUG("pq ~p (c=~b) deq worker: ~p to ~p", [self(), S#leader.capacity, Worker, Req]),
   gen_server:reply(Req, {ok, Worker}),
   S#leader{
      lq       = Lq,
      capacity = S#leader.capacity - 1
   }; 

deq_worker(S) ->
   {Worker, Wq} = deq:deq(S#leader.wq),
   case is_process_alive(Worker) of
      false ->
         deq_worker(S#leader{wq=Wq});
      true  ->
         {{lease, _, Req}, Lq} = deq:deq(S#leader.lq),
         ?DEBUG("pq ~p (c=~b) deq worker: ~p to ~p", [self(), S#leader.capacity, Worker, Req]),
         gen_server:reply(Req, {ok, Worker}),
         S#leader{
            lq       = Lq,
            wq       = Wq,
            capacity = S#leader.capacity - 1
         }
   end.

%%
%% increase / decrease capacity 
inc_capacity(#leader{}=S) ->
   S#leader{
      capacity = erlang:min(S#leader.capacity + 1, S#leader.size)
   }.   

dec_capacity(#leader{}=S) ->
   S#leader{
      capacity = erlang:max(S#leader.capacity - 1, 0)
   }.   


%%
%%
init_worker(Sup) ->
   {ok, Pid} = supervisor:start_child(Sup, []),
   erlang:monitor(process, Pid),
   ?DEBUG("pq ~p init worker: ~p", [self(), Pid]),
   Pid.

%%
%%
free_worker({}) ->
   ok;
free_worker(Q) ->
   Pid = deq:head(Q),
   ?DEBUG("pq ~p free worker: ~p", [self(), Pid]),
   erlang:exit(Pid, shutdown),
   free_worker(deq:tail(Q)).

