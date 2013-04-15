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
