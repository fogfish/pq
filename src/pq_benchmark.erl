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
   {ok, basho_bench_config:get(pq_type, disposable)}.
%%
%%
run(lease, _KeyGen, _ValGen, disposable) ->
   try
      {ok, Pid} = pq:lease(benq, 20000),
      _  = ping(Pid, exit),
      ok = pq:release(benq, Pid),
      {ok, disposable}
   catch _:_Reason ->
      {error, crash, disposable}
   end;

run(lease, _KeyGen, _ValGen, reusable) ->
   try
      {ok, Pid} = pq:lease(benq, 20000),
      _  = ping(Pid, ping),
      ok = pq:release(benq, Pid),
      {ok, reusable}
   catch _:_Reason ->
      {error, crash, reusable}
   end;

run(suspend, _KeyGen, _ValGen, S) ->
   try
      ok = pq:suspend(benq),
      timer:sleep(10000),
      ok = pq:resume(benq),
      {ok, S}
   catch _:_Reason ->
      {error, crash, S}
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
         Config0  = [{type, Type}, {capacity, Capacity}, {worker, pq_echo}],
         Config   = case basho_bench_config:get(pq_ondemand, false) of
            false -> Config0;
            true  -> [ondemand | Config0]
         end,
         pq:start_link(benq, Config)
   end.

%%
%%
ping(Pid, Msg) ->
   Pid ! {self(), Msg},
   receive
      Msg -> ok
   end.
