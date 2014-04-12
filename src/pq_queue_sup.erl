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
   init/1,
   client_api/1
]).

%%
-define(CHILD(Type, I),            {I,  {I, start_link,   []}, permanent, 30000, Type, dynamic}).
-define(CHILD(Type, I, Args),      {I,  {I, start_link, Args}, permanent, 30000, Type, dynamic}).
-define(CHILD(Type, ID, I, Args),  {ID, {I, start_link, Args}, permanent, 30000, Type, dynamic}).

%% 
%%
start_link(Name, Opts) ->
   {worker, Worker} = lists:keyfind(worker, 1, Opts),
   {ok, Sup} = supervisor:start_link(?MODULE, [Name, Opts]),
   Leader    = child(Sup, pq_leader),
   {ok, Pid} = supervisor:start_child(Sup, 
      ?CHILD(supervisor, pq_worker_sup, [Leader, Opts, Worker])
   ),
   ok = pq_leader:ioctl(Leader, {factory, Pid}),   
   {ok, Sup}.

init([Name, Opts]) ->   
   {ok,
      {
         {one_for_all, 0, 1},
         [
            %% queue leader
            ?CHILD(worker, pq_leader, [Name, Opts])
         ]
      }
   }.

%%
%%
child(Sup, Id) ->
   erlang:element(2,
      lists:keyfind(Id, 1, supervisor:which_children(Sup))
   ).

%%
%% return pid of client api
client_api(Sup) ->
   {ok, child(Sup, pq_leader)}.

