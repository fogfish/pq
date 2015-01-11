-module(spawnable).

-export([
   start_link/1
  ,start_link/2
  ,start_link/3
  ,close/1
  ,do/2
  ,do/3
  ,do_/2
  ,do_/3
]).

%%
%% start unit-of-work pool
start_link(N) ->
   pq:start_link([{type, reusable}, {capacity, N}, {worker, pq_spawnable}]).

start_link(Name, N) ->
   pq:start_link(Name, [{type, reusable}, {capacity, N}, {worker, pq_spawnable}]).

start_link(Name, Type, N) ->
   pq:start_link(Name, [{type, Type}, {capacity, N}, {worker, pq_spawnable}]).

%%
%% close unit of work
close(Pq) ->
   pq:close(Pq).

%%
%% synchronously evaluate functional object
-spec(do/2 :: (pid(), function()) -> any() | {error, any()}).
-spec(do/3 :: (pid(), function(), timeout()) -> any() | {error, any()}).

do(Pq, Fun) ->
   do(Pq, Fun, 5000).

do(Pq, Fun, Timeout) ->
   case pq:lease(Pq) of
      {error, _} = Error ->
         Error;
      Ref ->
         try
            gen_server:call(pq:pid(Ref), {do, Fun}, Timeout)
         catch _:Reason ->
            exit(Reason)
         after
            pq:release(Ref)
         end
   end.

%%
%% asynchronously evaluate functional object
-spec(do_/2 :: (pid(), function()) -> ok).
-spec(do_/3 :: (pid(), function(), boolean()) -> ok).

do_(Pq, Fun) ->
   do_(Pq, Fun, false).

do_(Pq, Fun, Flag) ->
   case pq:lease(Pq, [async]) of
      {error, ebusy} ->
         erlang:yield(),
         do_(Pq, Fun, Flag);
      Ref ->
         case Flag of
            false ->
               gen_server:cast(pq:pid(Ref), {do, Ref, Fun});
            true  ->
               Tx = erlang:make_ref(),
               gen_server:cast(pq:pid(Ref), {do, Ref, {self(), Tx}, Fun}),
               Tx
         end
   end.
