%% @description
%%   abstract pq worker
-module(pq_worker).

-export([
   start_link/1, init/2
]).

start_link(Fun) ->
    proc_lib:start_link(?MODULE, init, [self(), Fun]).

init(Parent, Fun) ->
   proc_lib:init_ack(Parent, {ok, self()}),
   Fun().
