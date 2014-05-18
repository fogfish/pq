-module(pq_tests).
-include_lib("eunit/include/eunit.hrl").

disposable_pool_test_() ->
   {foreach,
      fun() -> 
         erlang:element(2, 
            pq:start_link([{type, disposable}, {capacity, 2}, {worker, pq_echo}])
         ) 
      end,
      fun(Pool) -> pq:close(Pool) end,
      [
         fun lease/1
        ,fun release/1
        ,fun lifecycle/1
        ,fun out_of_capacity/1
        ,fun suspend_resume/1
      ]
   }.

reusable_pool_test_() ->
   {foreach,
      fun() -> 
         erlang:element(2, 
            pq:start_link([{type, reusable}, {capacity, 2}, {worker, pq_echo}])
         ) 
      end,
      fun(Pool) -> pq:close(Pool) end,
      [
         fun lease/1
        ,fun release/1
        ,fun lifecycle/1
        ,fun out_of_capacity/1
        ,fun suspend_resume/1
      ]
   }.

%%%------------------------------------------------------------------
%%%
%%% generic parameterizable test cases
%%%
%%%------------------------------------------------------------------

%%
%%
lease(Pool) ->
   [?_assertMatch({pq, _, _}, pq:lease(Pool))].

%%
%%
release(Pool) ->
   [?_assertMatch(ok, pq:release(pq:lease(Pool)))].

%%
%%
lifecycle(Pool) ->
   [
      ?_assertMatch(ok, pq:release(pq:lease(Pool)))
     ,?_assertMatch(ok, pq:release(pq:lease(Pool)))
     ,?_assertMatch(ok, pq:release(pq:lease(Pool)))
     ,?_assertMatch(ok, pq:release(pq:lease(Pool)))
     ,?_assertMatch(ok, pq:release(pq:lease(Pool)))
     ,?_assertMatch(ok, pq:release(pq:lease(Pool)))
   ].


%%
%%
out_of_capacity(Pool) ->
   [
      ?_assertMatch({pq, _, _}, pq:lease(Pool))
     ,?_assertMatch({pq, _, _}, pq:lease(Pool))
     ,?_assertMatch({error, ebusy}, pq:lease(Pool))
   ].

%%
%%
suspend_resume(Pool) ->
   [
      ?_assertMatch(ok, pq:suspend(Pool))
     ,?_assertMatch({error, ebusy}, pq:lease(Pool))
     ,?_assertMatch(ok, pq:resume(Pool))
     ,?_assertMatch({pq, _, _}, pq:lease(Pool))
   ].
