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
%%   echo example worker
-module(pq_echo).
-behaviour(gen_server).

-export([
   start_link/0
  ,init/1
  ,terminate/2
  ,handle_call/3
  ,handle_cast/2
  ,handle_info/2
  ,code_change/3
]).

%%
%% internal state
-record(srv, {}).

start_link() ->
   gen_server:start_link(?MODULE, [], []).

init([]) ->
   {ok, #srv{}}.

terminate(_, _State) ->
   ok.

%%
%%
handle_call(Req, _Tx, State) ->
   {reply, Req, State}.

%%
%%
handle_cast(_, State) ->
   {noreply, State}.

%%
%%
handle_info(_, State) ->
   {noreply, State}.

%%
%%
code_change(_OldVsn, State, _Extra) ->
   {ok, State}.


