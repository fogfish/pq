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
%%   pq library is process queue aka worker pool library.
%%   It supports strategies
%%      (N)-disposable queue maintain N disposable workers
%%      (N)-reusable   queue maintain N reusable workers
%%
-module(pq).

-include("pq.hrl").

-export([
   start/0
  ,start_link/1 
  ,start_link/2 
  ,close/1
  ,pid/1
  ,worker/1
  ,lease/1 
  ,lease/2 
  ,release/1
  ,suspend/1 
  ,resume/1
  ,ioctl/2
]).

%%
-type(pq()    :: atom() | pid()).
-type(token() :: any()).

%%
%% start application
start() ->
   application:start(?MODULE).

%%
%% start pool of processes
%%
%% Options:
%%   {worker,    atom() | {atom(), list()}} - worker specification
%%   {type,      disposable | reusable} - worker type
%%   {capacity,  integer()} - max number of workers
%%   {ttl,       integer()} - worker process time to live, enables process rotation (default 120.000 ms)
-spec(start_link/1 :: (list()) -> {ok, pid()} | {error, any()}).
-spec(start_link/2 :: (atom(), list()) -> {ok, pid()} | {error, any()}).

start_link(Opts) ->
   pq_pool:start_link(Opts).

start_link(Name, Opts) ->
   pq_pool:start_link(Name, Opts).

%%
%% close pool and terminate all workers
-spec(close/1 :: (pq()) -> ok).

close(Pq) ->
   pq_pool:close(Pq).

%%
%% return pid of worker process
-spec(pid/1 :: (token()) -> pid()).

pid(#pq{pid=Pid}) ->
   Pid;
pid({error, Reason}) ->
   exit(Reason).

%%
%% return worker specification
-spec(worker/1 :: (pq()) -> any()).

worker(Pq) ->
   pq_pool:worker(Pq).

%%
%% lease worker
%%  Options
%%   * async - use lease in asynchronous manner, do not monitor client
%%   * {tenant, pid()} - tenant process to monitor
-spec(lease/1  :: (pq()) -> {ok, pid()} | {error, any()}).
-spec(lease/2  :: (pq(), [atom()]) -> {ok, pid()} | {error, any()}).

lease(Pq) ->
   pq_pool:lease(Pq,   []).

lease(Pq, Opts) ->
   pq_pool:lease(Pq, Opts).

%%
%% release worker
-spec(release/1 :: (token()) -> ok).

release(#pq{}=Tx) ->
   pq_uow:release(Tx);
release({error, Reason}) ->
   exit(Reason);
release(undefined) ->
   ok.

%%
%% suspend queue and terminate all workers
-spec(suspend/1 :: (pq()) -> ok).

suspend(Pq) ->
   pq_pool:suspend(Pq).

%%
%% resume queues, enables lease requests and re-spawn workers
-spec(resume/1 :: (pq()) -> ok).

resume(Pq) ->
   pq_pool:resume(Pq).

%%
%% return property of process queue
%%   * capacity  - return total queue capacity
%%   * busy      - return number of occupied workers
%%   * free      - return number of free workers
-spec(ioctl/2 :: (pq(), atom()) -> any()).

ioctl(Pq, Req) ->
   pq_pool:ioctl(Pq, Req).



