%% @description
%%   pq library is process queue aka worker pool library.
%%
%%   queue strategies
%%      (N)-disposable queue maintain N disposable workers
%%      (N)-reusable   queue maintain N reusable workers
%%
-module(pq).
-include("pq.hrl").

-export([
   start_link/1, start_link/2, queue/1, 
   lease/1, lease/2, release/2,
   suspend/1, resume/1
]).
% -ifdef(DEBUG).
% -export([profile/0]).
% -endif.

%%
%%
-spec(start_link/1 :: (list()) -> {ok, pid()} | {error, any()}).
-spec(start_link/2 :: (atom(), list()) -> {ok, pid()} | {error, any()}).

start_link(Opts) ->
   pq_queue_sup:start_link(undefined, Opts).
   
start_link(Q, Opts) ->
   pq_queue_sup:start_link(Q, Opts).

%%
%%
-spec(queue/1 :: (pid()) -> pid()).

queue(Sup) ->
   pq_queue_sup:leader(Sup).

%%
%% lease worker
-spec(lease/1 :: (atom() | pid()) -> {ok, pid()} | {error, any()}).
-spec(lease/2 :: (atom() | pid(), integer() | infinity) -> {ok, pid()} | {error, any()}).

lease(Queue) ->
   lease(Queue, infinity).

lease(Queue, Timeout) ->
   try
      gen_server:call(Queue, {lease, Timeout}, Timeout)
   catch 
      exit:{timeout, _} -> {error, timeout}
   end.

%%
%% release worker
-spec(release/2 :: (atom() | pid(), pid()) -> ok | {error, any()}).

release(Queue, Pid) ->
   gen_server:call(Queue, {release, Pid}).

%%
%%
suspend(Queue) ->
   gen_server:call(Queue, suspend).

%%
%%
resume(Queue) ->
   gen_server:call(Queue, resume).



% -ifdef(DEBUG).
% %%
% profile() ->
%    application:start(pq),
%    {ok, _} = pq:start_link(q, [
%       {worker, fun() -> timer:sleep(1000) end}
%    ]),
%    fprof:trace([start, {procs, [erlang:whereis(q)]}]),
%    %% apply workload
%    lists:foreach(
%       fun(_)-> pq:release(q, erlang:element(2, pq:lease(q))) end,
%       lists:seq(1, 1000)
%    ),
%    %% get data
%    fprof:trace([stop]),
%    fprof:profile(),
%    fprof:analyse().
% -endif.