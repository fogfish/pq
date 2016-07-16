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
-module(pq).

-include("pq.hrl").

-export([
   start/0
  ,start_link/2 
  ,free/1
]).
-export([
   lease/1 
  ,release/2
]).
-export([
   call/2
  ,call/3
  ,cast/2
  ,send/2
]).


%%
%% start application
start() ->
   application:start(?MODULE).

%%
%% start pool of processes
%%
%% Options:
%%   {worker,    {atom(), list()}} - worker specification
%%   {strategy,  lifo | fifo | spawn} - worker re-use strategy
%%   {capacity,  integer()} - max number of workers
-spec start_link(atom(), list()) -> {ok, pid()} | {error, any()}.

start_link(Name, Opts) ->
   pq_pool_sup:start_link(Name, Opts).


%%
%% close pool and terminate all workers
-spec free(pid()) -> ok.

free(Pq) ->
   erlang:exit(Pq, shutdown).

%%
%% lease worker
-spec lease(atom()) -> {ok, pid()} | {error, any()}.

lease(Pq) ->
   pipe:call(Pq, lease, infinity).


%%
%% release worker
-spec release(atom(), pid()) -> ok.

release(Pq, Pid) ->
   pipe:send(Pq, {release, Pid}),
   ok.


%%
%% synchronously call worker
-spec call(atom(), _) -> _.
-spec call(atom(), _, timeout()) -> _.

call(Pq, Req) ->
   call(Pq, Req, 5000).

call(Pq, Req, Timeout) ->
   pipe:call(Pq, {forward, Req}, Timeout).


%%
%% asynchronous worker call 
-spec cast(atom(), _) -> reference().

cast(Pq, Req) ->
   pipe:cast(Pq, {forward, Req}).

%%
%% asynchronous send (fire-and-forget)
-spec send(atom(), _) -> ok.

send(Pq, Req) ->
   pipe:send(Pq, {forward, Req}), 
   ok.

