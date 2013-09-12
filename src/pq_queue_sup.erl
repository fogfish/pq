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
%%   queue / pool instance supervisor
-module(pq_queue_sup).
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
   supervisor:start_link(?MODULE, [Name, Opts]).
   
init([Name, Opts]) ->   
   % read worker specification
   Worker = case lists:keyfind(worker, 1, Opts) of
      {worker, {Mod, Args}} -> 
         {Mod, [self() | Args]};
      {worker, Mod} when is_atom(Mod) ->
         {Mod, [self()]}
   end,
   {ok,
      {
         {one_for_all, 4, 1800},
         [
            %% worker factory
            ?CHILD(supervisor, pq_worker_sup, [Worker])
            %% queue leader
           ,?CHILD(worker, pq_leader, [self(), Name, Opts])
         ]
      }
   }.

