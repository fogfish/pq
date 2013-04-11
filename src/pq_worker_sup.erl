%% @description
%%   pq worker factory
-module(pq_worker_sup).
-behaviour(supervisor).

-export([
   start_link/1, init/1
]).

%%
%%
start_link(Spec) ->
   supervisor:start_link(?MODULE, [Spec]).

init([Spec]) ->
   {ok,
      {
         {simple_one_for_one, 10, 60},
         [worker(Spec)]
      }
   }.

%%
%% build worker specification
worker(undefined) ->
   %% abstract workers specification, bound with assignment at allocation time
   {
      worker,
      {pq_worker, start_link, []},
      transient, 6000, worker, dynamic
   };

worker(Mod) 
 when is_atom(Mod) ->
   %% worker is module
   {
      worker, 
      {Mod, start_link, []},
      transient, 60000, worker, dynamic
   };

worker({M, F, A}) ->
   %% worker is {M, F, A} parametrized entity
   {
      worker, 
      {M, F, A},
      transient, 60000, worker, dynamic
   };

worker(Fun)
 when is_function(Fun) ->
   %% worker is predefined function
   {
      worker,
      {pq_worker, start_link, [Fun]},
      transient, 6000, worker, dynamic
   }.

