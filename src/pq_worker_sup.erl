-module(pq_worker_sup).
-behaviour(supervisor).

-export([
   start_link/1, init/1
]).

start_link(Spec) ->
   supervisor:start_link(?MODULE, [Spec]).

init([Spec]) ->
   {ok,
      {
         {simple_one_for_one, 10, 60},
         [worker(Spec)]
      }
   }.


worker(Mod) 
 when is_atom(Mod) ->
   {
      worker, 
      {Mod, start_link, []},
      transient, 60000, worker, dynamic
   };

worker({M, F, A}) ->
   {
      worker, 
      {M, F, A},
      transient, 60000, worker, dynamic
   };

worker(Fun)
 when is_function(Fun) ->
   {
      worker,
      {pq_worker, start_link, [Fun]},
      transient, 6000, worker, dynamic
   }.

