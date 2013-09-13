%% @description
%%   echo test worker
-module(pq_echo).

-export([
   start_link/0, 
   init/1
]).

start_link() ->
    proc_lib:start_link(?MODULE, init, [self()]).

init(Parent) ->
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
