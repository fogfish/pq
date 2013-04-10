-module(pq).
-include("pq.hrl").

-export([
   start_link/1, start_link/2, queue/1,
   lease/1, lease/2, release/2,
   suspend/1, resume/1
]).
-ifdef(DEBUG).
-export([profile/0]).
-endif.

%%
%%
-spec(start_link/1 :: (list()) -> {ok, pid()} | {error, any()}).
-spec(start_link/2 :: (atom(), list()) -> {ok, pid()} | {error, any()}).

start_link(Opts) ->
   pq_q_sup:start_link(undefined, Opts).
   
start_link(Q, Opts) ->
   pq_q_sup:start_link(Q, Opts).

%%
%%
-spec(queue/1 :: (pid()) -> pid()).

queue(Sup) ->
   pq_q_sup:leader(Sup).

%%
%% lease worker
-spec(lease/1 :: (atom()) -> {ok, pid()} | {error, any()}).
-spec(lease/2 :: (atom(), integer() | infinity) -> {ok, pid()} | {error, any()}).

lease(Q) ->
   lease(Q, infinity).

lease(Q, Timeout) ->
   pq_leader:lease(Q, Timeout).

%%
%% release worker
%-spec().

release(Q, Pid) ->
   pq_leader:release(Q, Pid).

%%
%% suspend queue
suspend(Q) ->
   pq_leader:suspend(Q).

resume(Q) ->
   pq_leader:resume(Q).


-ifdef(DEBUG).
%%
profile() ->
   application:start(pq),
   {ok, _} = pq:start_link(q, [
      {worker, fun() -> timer:sleep(1000) end}
   ]),
   fprof:trace([start, {procs, [erlang:whereis(q)]}]),
   %% apply workload
   lists:foreach(
      fun(_)-> pq:release(q, erlang:element(2, pq:lease(q))) end,
      lists:seq(1, 1000)
   ),
   %% get data
   fprof:trace([stop]),
   fprof:profile(),
   fprof:analyse().
-endif.