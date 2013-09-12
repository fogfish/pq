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
   lager:set_loglevel(lager_console_backend, basho_bench_config:get(log_level, info)),
   case erlang:whereis(benq) of
      undefined -> 
         application:start(pq),
         pq:start_link(benq, config());
      Pid ->
         {ok, Pid}
   end.

config() ->
   maybe_opt_config([
      {type,     basho_bench_config:get(pq_type, disposable)},
      {capacity, basho_bench_config:get(pq_capacity, 10)},
      {worker,   fun worker/0}
   ]).

maybe_opt_config(Cfg) ->
   case basho_bench_config:get(pq_ondemand, false) of
      false -> Cfg;
      true  -> [ondemand | Cfg]
   end.

%%
%%
run(lease, _KeyGen, _ValGen, S) ->
   try
      {ok, Pid} = pq:lease(benq, 20000),
      ping(Pid, ping),
      ok = pq:release(benq, Pid),
      {ok, S}
   catch _:_Reason ->
      %io:format("pq crash: ~p ~p~n", [Reason, erlang:get_stacktrace()]),
      {error, crash, S}
   end;

run(suspend, _KeyGen, _ValGen, S) ->
   try
      ok = pq:suspend(benq),
      timer:sleep(10000),
      ok = pq:resume(benq),
      {ok, S}
   catch _:_Reason ->
      %io:format("pq crash: ~p ~p~n", [Reason, erlang:get_stacktrace()]),
      {error, crash, S}
   end.      

%%
%% artificial queue worker
worker() ->
   Rnd = random:seed(erlang:now()),
   Lc  = basho_bench_config:get(pq_lifecycle, 100),
   worker_loop(basho_bench_config:get(pq_type, disposable), Lc).

worker_loop(disposable, Lc) ->
   receive
      {Pid,  Msg} ->
         timer:sleep(random:uniform(Lc)),
         Pid ! Msg
   end;

worker_loop(reusable, Lc) ->
   receive
      {Pid, exit} ->
         Pid ! exit;
      {Pid,  Msg} ->
         timer:sleep(random:uniform(Lc)),
         Pid ! Msg,
         worker_loop(reusable, Lc)
   end.

%%
ping(Pid, Msg) ->
   Pid ! {self(), Msg},
   receive
      Msg -> ok
   end.
