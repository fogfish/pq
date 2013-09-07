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
   start_link/1, 
   start_link/2, 
   lease/1, 
   lease/2, 
   release/2,
   suspend/1, 
   resume/1,
   leader/1 
]).

%%
-type(pq() :: atom() | pid()).

%%
%% start pool of processes
%% Options:
%%   {worker,    atom() | {atom(), list()}} - worker specification
%%   {type,      disposable | reusable} - worker type
%%   {capacity,  integer()} - max number of workers
%%   {linger,    integer()} - max number of delayed lease requests
%%   {ondemand,  boolean()} - worker pre-allocation strategy
-spec(start_link/1 :: (list()) -> {ok, pid()} | {error, any()}).
-spec(start_link/2 :: (atom(), list()) -> {ok, pid()} | {error, any()}).

start_link(Opts) ->
   pq_queue_sup:start_link(undefined, Opts).
   
start_link(Name, Opts) ->
   pq_queue_sup:start_link(Name, Opts).

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
%% suspend queue, disable lease requests and terminate all workers
-spec(suspend/1 :: (pq()) -> ok).

suspend(Pq) ->
   gen_server:call(Pq, suspend).

%%
%% resume queues, enables lease requests and re-spawn workers
-spec(resume/1 :: (pq()) -> ok).

resume(Pq) ->
   gen_server:call(Pq, resume).


%%
%% return pid of queue leader process
-spec(leader/1 :: (pid()) -> pid()).

leader(Sup) ->
   {_, Pid, _, _} = lists:keyfind(pq_leader, 1, supervisor:which_children(Sup)),
   Pid.

