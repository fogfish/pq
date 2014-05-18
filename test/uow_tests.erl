-module(uow_tests).
-include_lib("eunit/include/eunit.hrl").

uow_test_() ->
   {foreach,
      fun() -> 
         erlang:element(2, uow:start_link(2)) 
      end,
      fun(Pool) -> uow:close(Pool) end,
      [
         fun do/1
        ,fun fail/1
      ]
   }.


%%
%%
do(Pool) ->
   ?_assertMatch(ok, uow:do(Pool, fun() -> ok end)).

%%
%%
fail(Pool) ->
   ?_assertMatch({'EXIT', fail}, uow:do(Pool, fun() -> exit(fail) end)).
