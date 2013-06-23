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
   suspend/1, resume/1, touch/1, touch/2
]).

-type(pq() :: atom() | pid()).

% -ifdef(DEBUG).
% -export([profile/0]).
% -endif.

%%
%%
-spec(start_link/1 :: (list()) -> {ok, pid()} | {error, any()}).
-spec(start_link/2 :: (atom(), list()) -> {ok, pid()} | {error, any()}).

start_link(Opts) ->
   pq_queue_sup:start_link(undefined, Opts).
   
start_link(Name, Opts) ->
   pq_queue_sup:start_link(Name, Opts).

%%
%%
-spec(queue/1 :: (pid()) -> pid()).

queue(Sup) ->
   pq_queue_sup:leader(Sup).

%%
%% lease worker
-spec(lease/1 :: (pq()) -> {ok, pid()} | {error, any()}).
-spec(lease/2 :: (pq(), integer() | infinity) -> {ok, pid()} | {error, any()}).

lease(Pq) ->
   lease(Pq, infinity).

lease(Pq, Timeout) ->
   gen_server:call(Pq, {lease, Timeout}, Timeout).

%%
%% release worker
-spec(release/2 :: (pq(), pid()) -> ok | {error, any()}).

release(Pq, Pid) ->
   gen_server:call(Pq, {release, Pid}).

%%
%% suspend queue
suspend(Pq) ->
   gen_server:call(Pq, suspend).

%%
%% resume queues
resume(Pq) ->
   gen_server:call(Pq, resume).

%%
%% touch queue (leases & releases queue head)
-spec(touch/1 :: (pq()) -> {ok, pid()} | {error, any()}).
-spec(touch/2 :: (pq(), timeout()) -> {ok, pid()} | {error, any()}).

touch(Pq) ->
   touch(Pq, infinity).

touch(Pq, Timeout) ->
   {ok, Pid} = pq:lease(Pq, Timeout),
   ok = pq:release(Pq, Pid),
   {ok, Pid}.

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