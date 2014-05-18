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
%%   basho bench driver
-module(pq_benchmark).

-export([new/1, run/4]).

%%
%%
new(_Id) ->
   try
      lager:set_loglevel(lager_console_backend, basho_bench_config:get(log_level, info)),
      init()
   catch _:Err ->
      lager:error("pq failed: ~p", [Err]),
      halt(1)
   end,
   {ok, q:new()}.

%%
%%
run(spawn, _KeyGen, _ValGen, Type) ->
   %% uses by-pass pq, uses erlang spawn
   try
      {ok, Pid} = pq_echo:start(self()),
      % Pid = spawn(
      %    fun() -> 
      %       receive
      %          {Pid, exit} -> Pid ! exit
      %       end
      %    end
      % ),
      _  = ping(Pid, exit),
      {ok, disposable}
   catch _:Reason ->
      {error, Reason, Type}
   end;

run(request, _KeyGen, _ValGen, State) ->
   case pq:lease(benq) of
      {error, ebusy} ->
         {error, ebusy, State};
      Ref ->
         _ = ping(pq:pid(Ref), ping),
         pq:release(Ref),
         {ok, State}
   end;

run(lease, _KeyGen, _ValGen, State) ->
   case pq:lease(benq) of
      {error, ebusy} ->
         {error, ebusy, State};
      Ref ->
         _ = ping(pq:pid(Ref), ping),
         {ok, q:enq(Ref, State)}
   end;

run(release, _KeyGen, _ValGen, State) ->
   case q:length(State) of
      0 ->
         {error, empty, State};
      _ ->
         pq:release(q:head(State)),
         {ok, q:tail(State)}
   end;

run(crash, _KeyGen, _ValGen, State) ->
   case pq:lease(benq) of
      {error, ebusy} ->
         {error, ebusy, State};
      Ref ->
         _ = ping(pq:pid(Ref), exit),
         pq:release(Ref),
         {ok, State}
   end.

%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%%
init() ->
   case application:start(pq) of
      {error, {already_started, _}} ->
         ok;
      ok ->
         Type     = basho_bench_config:get(pq_type, disposable),
         Capacity = basho_bench_config:get(pq_capacity, 10),
         % spawn(fun() ->
         %    eep:start_file_tracing("pqt"), 
         %    timer:sleep(10000), 
         %    eep:stop_tracing()
         % end),
         pq:start_link(benq, [{type, Type}, {capacity, Capacity}, {worker, pq_echo}])
   end.

%%
%%
ping(Pid, Msg) ->
   Pid ! {self(), Msg},
   receive
      Msg -> ok
   end.
