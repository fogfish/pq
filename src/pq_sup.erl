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
         {simple_one_for_one, 4, 1800},
         [q()]
      }
   }.

q() ->
   {
      q,
      {pq_q_sup, start_link, []},
      transient, 60000, supervisor, dynamic
   }.