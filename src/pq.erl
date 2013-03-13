-module(pq).

-export([
   start_link/2, lease/1, lease/2, release/2
]).

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