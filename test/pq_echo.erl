%% @description
%%   echo test worker
-module(pq_echo).

-export([
   start_link/1, 
   init/2
]).

start_link(Queue) ->
    proc_lib:start_link(?MODULE, init, [self(), Queue]).

init(Parent, _Queue) ->
   proc_lib:init_ack(Parent, {ok, self()}),
   loop().

loop() ->
   receive
      {Pid, exit} ->
         Pid ! exit;
      {Pid,  Msg} ->
         Pid ! Msg,
         loop()
   end.
