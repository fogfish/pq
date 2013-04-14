-module(pq_util).

-export([
   usec/0, deadline/1, expired/1
]).


%%
%%
usec() ->
   {Mega, Sec, Micro} = erlang:now(),
   (Mega * 1000000 + Sec) * 1000000 + Micro.

%%
%%
deadline(infinity) ->
   infinity;
deadline(Timeout)     ->
   usec() + Timeout * 1000.

%%
%% check is queued entity is expired
expired({Deadline, _}) ->
   usec() >= Deadline.
