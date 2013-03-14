-module(pq).
-include("pq.hrl").

-export([
   start_link/2, lease/1, lease/2, release/2
]).
-ifdef(DEBUG).
-export([profile/0]).
-endif.

%%
%%
-spec(start_link/2 :: (atom(), list()) -> {ok, pid()} | {error, any()}).

start_link(Q, Opts) ->
   pq_q_sup:start_link(Q, Opts).

%%
%%
-spec(lease/1 :: (atom()) -> {ok, pid()} | {errpr, any()}).
-spec(lease/2 :: (atom(), integer() | infinity) -> {ok, pid()} | {errpr, any()}).

lease(Q) ->
   lease(Q, infinity).

lease(Q, Timeout) ->
   pq_leader:lease(Q, Timeout).

%%
%%
%-spec().

release(Q, Pid) ->
   pq_leader:release(Q, Pid).


-ifdef(DEBUG).
%%
profile() ->
   {ok, _} = pq:start_link(q, [
      {worker, fun() -> timer:sleep(1000) end}
   ]),
   fprof:trace([start, {procs, [erlang:whereis(q)]}]),
   %% apply workload
   lists:foreach(
      fun(_)-> pq:release(q, erlang:element(2, pq:lease(q))) end,
      lists:seq(1, 10000)
   ),
   %% get data
   fprof:trace([stop]),
   fprof:profile(),
   fprof:analyse().
-endif.