%% @description
%%    process queue
-module(pq_process).

-export([
   new/2, out/1, free/1, init/2, evict/2, spawn/2
]).

-record(q, {
   c       :: integer(),   % queue allocated capacity
   size    :: integer(),   % queue max size
   spawner :: function(),  % spawner function
   i       :: any()        % queue instance
}).

%%
%%
new(Size, Fun) ->
   #q{
      c       = 0,
      size    = Size,
      spawner = Fun,
      i       = queue:new()
   }.

%%
%% peek first valid element
out({{value, Pid}, #q{c=C, i=I}=Q}) ->
   case is_process_alive(Pid) of
      true -> 
         {Pid, Q};
      _    -> 
         {Head, Tail} = queue:out(I),
         out({Head, Q#q{c=C-1, i=Tail}})
   end;

out({empty, #q{c=C, size=Size, spawner=Fun}=Q})
 when C < Size ->
   {Fun(), Q#q{c=C + 1}};

out({empty, Q}) ->
   {undefined, Q};

out(#q{i=I}=Q) ->
   {Head, Tail} = queue:out(I),
   out({Head, Q#q{i=Tail}}).


%%
%% terminate all queued processes 
free(#q{i=I}=Q) ->
   [erlang:exit(Pid, shutdown) || Pid <- queue:to_list(I)],
   Q#q{
      c = 0,
      i = queue:new()
   }.

%%
%% init capacity
init(N, #q{c=C, size=Size, i=I, spawner=Fun}=Q)
 when C + N =< Size ->
   Q#q{
      i = lists:foldl(fun(_, Acc) -> queue:in(Fun(), Acc) end, I, lists:seq(1, N)),
      c = C + N
   }.

%%
%% evict unit of process capacity
evict(_Pid, #q{c=C}=Q) ->
   Q#q{
      c = C - 1
   }.

%%
%% re-spawn unit of process capacity
spawn(_Pid, #q{i=I, spawner=Fun}=Q) ->
   Q#q{
      i = queue:in(Fun(), I)
   }.



