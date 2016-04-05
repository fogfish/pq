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
   pid/1
  ,lease/1 
  ,release/1
]).
-export([
   call/2
  ,call/3
  ,cast/2
  ,send/2
]).

%%
%% data types
-type pq() :: #pq{}.


%%
%% start application
start() ->
   application:start(?MODULE).

%%
%% start pool of processes
%%
%% Options:
%%   {worker,    {atom(), list()}} - worker specification
%%   {strategy,  lifo | fifo} - worker re-use strategy
%%   {capacity,  integer()} - max number of workers
-spec(start_link/2 :: (atom(), list()) -> {ok, pid()} | {error, any()}).

start_link(Name, Opts) ->
   pq_pool_sup:start_link(Name, Opts).


%%
%% close pool and terminate all workers
-spec(free/1 :: (pid()) -> ok).

free(Pq) ->
   erlang:exit(Pq, shutdown).


%%
%% return pid of worker process
-spec(pid/1 :: (pq()) -> pid()).

pid(#pq{pid=Pid}) ->
   Pid;
pid({error, Reason}) ->
   exit(Reason).


%%
%% lease worker
-spec(lease/1  :: (atom()) -> {ok, pq()} | {error, any()}).

lease(Pq) ->
   pipe:call(Pq, lease, infinity).


%%
%% release worker
-spec(release/1 :: (pq()) -> ok).

release(#pq{pool=Pool, pid=Pid}) ->
   pipe:send(Pool, {release, Pid});
release({error, Reason}) ->
   exit(Reason);
release(undefined) ->
   ok.


%%
%% synchronously call worker
-spec(call/2 :: (atom(), _) -> _).
-spec(call/3 :: (atom(), _, timeout()) -> _).

call(Pq, Req) ->
   call(Pq, Req, 5000).

call(Pq, Req, Timeout) ->
   pipe:call(Pq, {forward, Req}, Timeout).


%%
%% asynchronous worker call 
-spec(cast/2 :: (atom(), _) -> reference()).

cast(Pq, Req) ->
   pipe:cast(Pq, {forward, Req}).

%%
%% asynchronous send (fire-and-forget)
-spec(send/2 :: (atom(), _) -> ok).

send(Pq, Req) ->
   pipe:send(Pq, {forward, Req}), 
   ok.

