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
   start_link/5
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
  ,lease/3
  ,release/1
]).

%% internal state
-record(fsm, {
   pool     = undefined :: pid()       %% pool process
  ,client   = undefined :: reference() %% client reference
  ,type     = undefined :: atom()      %% worker re-use strategy
  ,protocol = undefined :: atom()      %% worker ipc protocol
  ,worker   = undefined :: any()       %% worker process specification
  ,ttl      = undefined :: any()       %% worker process timeout
  ,pid      = undefined :: pid()       %% worker process instance
}).

%%%------------------------------------------------------------------
%%%
%%% factory
%%%
%%%------------------------------------------------------------------

start_link(Pool, Type, Prot, TTL, Worker) ->
   gen_fsm:start_link(?MODULE, [Pool, Type, Prot, TTL, Worker], []).

init([Pool, Type, Prot, TTL, Worker]) ->
   erlang:process_flag(trap_exit, true),
   {ok, idle,
      #fsm{
         pool     = Pool
        ,type     = Type
        ,protocol = Prot
        ,ttl      = TTL
        ,worker   = Worker
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
lease(Pid, Tx, Req) ->
   gen_fsm:send_event(Pid, {lease, Tx, Req}).

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
idle({lease, Tx, _}=Req, #fsm{pid=undefined}=State) ->
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

idle({lease, {Pid, _}=Tx, {lease, Opts}}, State) ->
   Ref = client_monitor(Pid, Opts),
   gen_server:reply(Tx, #pq{uow=self(), pid=State#fsm.pid}),
   {next_state, inuse, 
      State#fsm{
         client = Ref
      }
   };

idle({lease, Tx, {call, Req, Timeout}}, #fsm{type=disposable, pid=Pid}=State) ->
   (catch gen_server:reply(Tx, gen_server:call(Pid, Req, Timeout))),
   pq_pool:release(State#fsm.pool, self()),
   _ = free_worker(State#fsm.pid),
   {next_state, idle, State#fsm{pid=undefined}};

idle({lease, Tx, {call, Req, Timeout}}, #fsm{pid=Pid}=State) ->
   (catch gen_server:reply(Tx, gen_server:call(Pid, Req, Timeout))),
   pq_pool:release(State#fsm.pool, self()),
   {next_state, idle, State};

idle({release, _}, State) ->
   {next_state, idle, State}.

%%
%%
inuse({lease, Tx, _}, State) ->
   gen_server:reply(Tx, {error, ebusy}),
   {next_state, inuse, State};

inuse({release, _}, #fsm{type=disposable, client = Client}=State) ->
   ?DEBUG("pq [uow]: ~p free worker ~p~n", [State#fsm.pool, State#fsm.pid]),      
   client_demonitor(Client),
   pq_pool:release(State#fsm.pool, self()),
   _ = free_worker(State#fsm.pid),
   {next_state, idle, State#fsm{pid=undefined}};
   
inuse({release, _}, #fsm{client = Client} = State) ->
   client_demonitor(Client),
   pq_pool:release(State#fsm.pool, self()),
   {next_state, idle, State}.

%%
%%
expired({release, _}, #fsm{client = Client} = State) ->
   ?DEBUG("pq [uow]: ~p free worker ~p~n", [State#fsm.pool, State#fsm.pid]),
   client_demonitor(Client),    
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

handle_info({'EXIT', _Pid, _Reason}, idle, State) ->
   ?DEBUG("pq [uow]: ~p death of worker ~p due ~p~n", [State#fsm.pool, _Pid, _Reason]),      
   {next_state, idle, State#fsm{pid=undefined}};

handle_info({'EXIT', _Pid, _Reason}, inuse, State) ->
   ?DEBUG("pq [uow]: ~p death of worker ~p due ~p~n", [State#fsm.pool, _Pid, _Reason]),      
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

%%
%%
client_monitor(Pid0, Opts) ->
   case lists:member(async, Opts) of
      false ->
         case lists:keyfind(tenant, 1, Opts) of
            false     ->
               erlang:monitor(process, Pid0);
            {_, Pid1} ->
               erlang:monitor(process, Pid1)
         end;
      true  ->
         undefined
   end.

%%
%%
client_demonitor(undefined) ->
   ok;
client_demonitor(Ref) ->
   erlang:demonitor(Ref, [flush]).   






