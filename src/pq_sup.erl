%% @description
%%    manage client queues
-module(pq_sup).
-behaviour(supervisor).

-export([
   start_link/0, init/1
]).

%%
%%
start_link() ->
   supervisor:start_link({local, ?MODULE}, ?MODULE, []).
   
init([]) ->   
   {ok,
      {
         {one_for_one, 4, 1800},
         []
      }
   }.

