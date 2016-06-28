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
-behaviour(pipe).
-include("pq.hrl").

-export([
   start_link/3
  ,init/1
  ,free/2
  ,handle/3
]).

-record(pool, {
   wq       = undefined   :: queue:queue()  % queue of worker processes
  ,size     = 0           :: integer()      % worker queue size
  ,capacity = 10          :: integer()      % worker queue capacity
  ,strategy = fifo        :: fifo | lifo    % dequeue strategy
  ,worker   = undefined   :: [_]            % worker arguments
  ,sup      = undefined   :: pid()          % worker factory
}).

%%%------------------------------------------------------------------
%%%
%%% factory
%%%
%%%------------------------------------------------------------------

start_link(Name, Sup, Opts) ->
   pipe:start_link({local, Name}, ?MODULE, [Sup, Opts], []).

%%
init([Sup, Opts]) ->
   erlang:process_flag(trap_exit, true),
   {ok, handle, 
      init(Opts, #pool{sup = Sup, wq = queue:new()})
   }.

init([{capacity, X} | Opts], State) ->
   init(Opts, State#pool{capacity=X});

init([{strategy, X} | Opts], State) ->
   init(Opts, State#pool{strategy=X});

init([{worker, {_, X}} | Opts], State) ->
   init(Opts, State#pool{worker=X});

init([_ | Opts], State) ->
   init(Opts, State);

init([], State) ->
   State.

%%
free(_, _State) ->
   ok.

%%%------------------------------------------------------------------
%%%
%%% pipe
%%%
%%%------------------------------------------------------------------

%%
%%
handle(lease, Pipe, State0) ->
   case deq(State0) of
      {{value, Pid}, State1} ->
         pipe:ack(Pipe, {ok, Pid}),
         {next_state, handle, State1};
      {empty, State1} ->
         pipe:ack(Pipe, {error, ebusy}),
         {next_state, handle, State1}
   end;


handle({release, Pid}, Pipe, State0) ->
   pipe:ack(Pipe, ok),
   {next_state, handle, enq(Pid, State0)};


handle({forward, Msg}, Pipe, State0) ->
   case deq(State0) of
      {{value, Pid}, State1} ->
         pipe:emit(Pipe, Pid, Msg),
         {next_state, handle, State1};
      {empty, State1} ->
         pipe:ack(Pipe, {error, ebusy}),
         {next_state, handle, State1}
   end;

handle({'EXIT', Pid, _}, _Pipe, State0) ->
   {next_state, handle, drop(Pid, State0)}.


%%%------------------------------------------------------------------
%%%
%%% private
%%%
%%%------------------------------------------------------------------

%%
%%
deq(#pool{strategy = spawn} = State) ->
   new(State);   

deq(#pool{wq = Wq0} = State) ->
   case queue:out(Wq0) of
      {{value, _} = Value, Wq1} ->
         {Value, State#pool{wq = Wq1}};
      {empty,  _} ->
         new(State)
   end.

%%
%%
enq(Pid, #pool{strategy = fifo, wq = Wq0} = State) ->
   State#pool{wq = queue:in(Pid, Wq0)};

enq(Pid, #pool{strategy = lifo, wq = Wq0} = State) ->
   State#pool{wq = queue:in_r(Pid, Wq0)};

enq(_,   #pool{strategy = spawn} = State) ->
   State.


%%
%%
new(#pool{size = Size, capacity = Capacity} = State)
 when Size =:= Capacity ->
   {empty, State};

new(#pool{sup = Sup, size = Size, worker = Worker} = State) ->
   case supervisor:start_child(Sup, [self() | Worker]) of
      % worker is created successfully
      {ok, Pid} ->
         erlang:link(Pid),
         {{value, Pid}, State#pool{size = Size + 1}};
      % unable to create worker
      _ ->
         {empty, State}
   end.

%%
%%
drop(Pid, #pool{wq = Wq0, size = Size} = State) ->
   Wq1 = queue:filter(fun(X) -> X =/= Pid end, Wq0),
   State#pool{wq = Wq1, size = Size - 1}.
