%% @description
%%    pq inbox of incoming allocation requests 
-module(pq_queue).

-export([
   new/0, in/2, in/3, out/1 
]).

%%
%% new lease queue
new() ->
   queue:new().

%%
%%
in(Item, Q) ->
   queue:in({infinity, Item}, Q).

in(Item, Timeout, Q)
 when is_number(Timeout) ->
   queue:in({pq_util:usec() + Timeout * 1000, Item}, Q);

in(Item, _, Q) ->
   in(Item, Q).


%%
%% peek first valid element
out({{value, {Expire, Item}}, Q}) ->
   case pq_util:usec() of
      Now when Now < Expire -> 
         {Item, Q};
      _ -> 
         out(queue:out(Q))
   end;

out({empty, Q}) ->
   {undefined, Q};

out(Q) ->
   out(queue:out(Q)).

