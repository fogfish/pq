-module(spawnable_tests).
-include_lib("eunit/include/eunit.hrl").

uow_test_() ->
   {foreach,
      fun() -> 
         erlang:element(2, spawnable:start_link(2)) 
      end,
      fun(Pool) -> spawnable:close(Pool) end,
      [
         fun do/1
        ,fun fail/1
      ]
   }.


%%
%%
do(Pool) ->
   ?_assertMatch(ok, spawnable:do(Pool, fun() -> ok end)).

%%
%%
fail(Pool) ->
   ?_assertMatch({'EXIT', fail}, spawnable:do(Pool, fun() -> exit(fail) end)).
