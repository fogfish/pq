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
   config_reusable(
      config_throttle(
         config_worker(
            [{length, basho_bench_config:get(pq_length, 10)}]
         )
      )
   ).

config_reusable(Opts) ->
   case basho_bench_config:get(pq_reusable, false) of
      false -> Opts;
      true  -> [reusable | Opts]
   end.

config_throttle(Opts) ->
   case basho_bench_config:get(pq_throttle, false) of
      false -> Opts;
      true  -> [throttle | Opts]
   end.

config_worker(Opts) ->
   T = basho_bench_config:get(pq_lifecycle, 10000),
   [{worker, fun() -> timer:sleep(T) end} | Opts].

%%
%%
run(lease, _KeyGen, _ValGen, S) ->
   try
      {ok, Pid} = pq:lease(benq, 20000),
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





