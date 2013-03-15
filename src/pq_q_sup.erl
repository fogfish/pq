-module(pq_q_sup).
-behaviour(supervisor).

-export([
   start_link/2, init/1, leader/1, worker/1
]).

%%
%%
start_link(Q, Opts) ->
   supervisor:start_link(?MODULE, [Q, Opts]).
   
init([Q, Opts]) ->   
   {ok,
      {
         {one_for_all, 4, 1800},
         [worker(Q, Opts), leader(Q, Opts)]
      }
   }.

%%
leader(Sup) ->
   {_, Pid, _, _} = lists:keyfind(leader, 1, supervisor:which_children(Sup)),
   {ok, Pid}.

%%
worker(Sup) ->
   {_, Pid, _, _} = lists:keyfind(worker, 1, supervisor:which_children(Sup)),
   {ok, Pid}.


%%
leader(Q, Opts) ->
   {
      leader,
      {pq_leader, start_link, [self(), Q, Opts]},
      permanent, 60000, worker, dynamic
   }.

%%
worker(_, Opts) ->
   {worker, Worker} = lists:keyfind(worker, 1, Opts),
   {
      worker,
      {pq_worker_sup, start_link, [Worker]},
      permanent, 30000, supervisor, dynamic
   }.
