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
%%   basho bench driver
-module(pq_benchmark).

-export([new/1, run/4]).

%%
%%
new(_Id) ->
   try
      lager:set_loglevel(lager_console_backend, basho_bench_config:get(log_level, info)),
      {ok, init()}
   catch _:Err ->
      lager:error("pq failed: ~p", [Err]),
      halt(1)
   end.

%%
%%
run(request, _KeyGen, _ValGen, State) ->
   case pq:lease(benq) of
      {error, ebusy} ->
         {error, ebusy, State};
      {ok, Ref} ->
         _ = ping(pq:pid(Ref), ping),
         pq:release(Ref),
         {ok, State}
   end;

run(crash, _KeyGen, _ValGen, State) ->
   case pq:lease(benq) of
      {error, ebusy} ->
         {error, ebusy, State};
      {ok, Ref} ->
         _ = ping(pq:pid(Ref), exit),
         pq:release(Ref),
         {ok, State}
   end.

%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%%
init() ->
   application:start(pq),
   Capacity = basho_bench_config:get(pq_capacity, 10),
   pq:start_link(benq, [
      {capacity, Capacity}, 
      {worker,   {pq_echo, []}}
   ]).


%%
%%
ping(Pid, Msg) ->
   Pid ! {self(), Msg},
   receive
      Msg -> ok
   end.
