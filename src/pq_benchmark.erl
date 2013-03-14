-module(pq_benchmark).

-export([new/1, run/4]).

%%
%%
new(_Id) ->
   lager:set_loglevel(lager_console_backend, basho_bench_config:get(log_level, info)),
   case erlang:whereis(benq) of
      undefined -> 
         pq:start_link(benq, config());
      Pid ->
         {ok, Pid}
   end.

config() ->
   Len = basho_bench_config:get(pq_length, 10),
   case basho_bench_config:get(pq_reusable, false) of
      false -> [{length, Len}, worker()];
      true  -> [{length, Len}, worker(), reusable]
   end.

worker() ->
   T = basho_bench_config:get(pq_lifecycle, 10000),
   {worker, fun() -> timer:sleep(T) end}.

%%
%%
run(lease, _KeyGen, _ValGen, S) ->
   {ok, Pid} = pq:lease(benq, 1000),
   ok = pq:release(benq, Pid),
   {ok, S}.


