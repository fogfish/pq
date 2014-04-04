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
%%   queue / pool worker factory
-module(pq_worker_sup).
-behaviour(supervisor).

-export([
   start_link/3,
   init/1
]).

%%
-define(CHILD(Type, I),            {I,  {I, start_link,   []}, transient, 30000, Type, dynamic}).
-define(CHILD(Type, I, Args),      {I,  {I, start_link, Args}, transient, 30000, Type, dynamic}).
-define(CHILD(Type, I, F, A),      {I,  {I,          F,    A}, transient, 30000, Type, dynamic}).

%%
%%
start_link(Leader, Opts, {Mod, Fun, Args}) -> 
   supervisor:start_link(?MODULE, [Mod, Fun, args(Leader, Opts, Args)]);
start_link(Leader, Opts, {Mod, Args}) ->
   supervisor:start_link(?MODULE, [Mod, start_link, args(Leader, Opts, Args)]);
start_link(Leader, Opts, Mod) ->
   supervisor:start_link(?MODULE, [Mod, start_link, args(Leader, Opts, [])]).

init([Mod, Fun, Args]) ->
   {ok,
      {
         {simple_one_for_one, 10, 60},
         [
            ?CHILD(worker, Mod, Fun, Args)
         ]
      }
   }.

%%
%%  
args(Leader, Opts, Args) ->
   case proplists:get_value(external, Opts) of
      true -> Args;
      _    -> [Leader | Args]
   end.
