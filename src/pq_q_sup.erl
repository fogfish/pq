-module(pq_q_sup).
-behaviour(supervisor).

-export([
   start_link/2, init/1
]).

%%
%%
start_link(Q, Opts) ->
   supervisor:start_link(?MODULE, [Q, Opts]).
   
init([Q, Opts]) ->   
   {ok,
      {
         {one_for_all, 4, 1800},
         [leader(Q, Opts)]
      }
   }.

%%
leader(Q, Opts) ->
   {
      leader,
      {pq_leader, start_link, [self(), Q, Opts]},
      permanent, 60000, worker, dynamic
   }.

   


