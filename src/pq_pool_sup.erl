%%
%%   Copyright (c) 2012 - 2016, Dmitry Kolesnikov
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
%% @doc
%%   pool supervisor
-module(pq_pool_sup).
-behaviour(supervisor).

-export([
   start_link/2,
   init/1
]).

%%
-define(CHILD(Type, I),            {I,  {I, start_link,   []}, permanent, 30000, Type, dynamic}).
-define(CHILD(Type, I, Args),      {I,  {I, start_link, Args}, permanent, 30000, Type, dynamic}).
-define(CHILD(Type, ID, I, Args),  {ID, {I, start_link, Args}, permanent, 30000, Type, dynamic}).


%%
%%
start_link(Name, Opts) ->
   {worker, {Mod, _}} = lists:keyfind(worker, 1, Opts),
   {ok, Sup} = supervisor:start_link(?MODULE, []),
   {ok, Uow} = supervisor:start_child(Sup,
      ?CHILD(supervisor, sup, pq_uow_sup, [Mod])
   ),
   {ok,   _} = supervisor:start_child(Sup,
      ?CHILD(worker, man, pq_pool, [Name, Uow, Opts])
   ),
   {ok, Sup}.
   
init([]) ->
   {ok,
      {
         {one_for_one, 0, 1},
         []
      }
   }.
