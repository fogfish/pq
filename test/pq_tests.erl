-module(pq_tests).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_DISPOSABLE,          [{type, disposable},{capacity,2},{worker,pq_echo}]).
-define(TEST_DISPOSABLE_ONDEMAND, [{type, disposable},{capacity,2},{worker,pq_echo},ondemand]).
-define(TEST_REUSABLE,            [{type, reusable},  {capacity,2},{worker,pq_echo}]).
-define(TEST_REUSABLE_ONDEMAND,   [{type, reusable},  {capacity,2},{worker,pq_echo},ondemand]).

%%
%% disposable q
-ifdef(TEST_DISPOSABLE).
dq_test_() ->
   {
      setup,
      fun() -> q_init(?TEST_DISPOSABLE) end,
      fun q_free/1,
      [
         {"lease/release",   fun q_lease_release/0}
        ,{"out-of-capacity", fun q_ooc/0}
        ,{"lease timeout",   fun q_lease_timeout/0}
        ,{"suspend/resume",  fun q_suspend_resume/0}
      ]
   }.
-endif.

%%
%% disposable ondemand q
-ifdef(TEST_DISPOSABLE_ONDEMAND).
dq_ondemand_test_() ->
   {
      setup,
      fun() -> q_init(?TEST_DISPOSABLE_ONDEMAND) end,
      fun q_free/1,      
      [
         {"lease/release",   fun q_lease_release/0}
        ,{"out-of-capacity", fun q_ooc/0}
        ,{"lease timeout",   fun q_lease_timeout/0}
        ,{"suspend/resume",  fun q_suspend_resume/0}
      ]
   }.
-endif.

%%
%% reusable q
-ifdef(TEST_REUSABLE).
reusable_test_() ->
   {
      setup,
      fun() -> q_init(?TEST_REUSABLE) end,
      fun q_free/1,      
      [
         {"lease/release",   fun q_lease_release/0}
        ,{"out-of-capacity", fun q_ooc/0}
        ,{"lease timeout",   fun q_lease_timeout/0}
        ,{"suspend/resume",  fun q_suspend_resume/0}
      ]
   }.
-endif.

%%
%% reusable ondemand q
-ifdef(TEST_REUSABLE_ONDEMAND).
reusable_ondemand_test_() ->
   {
      setup,
      fun() -> q_init(?TEST_REUSABLE_ONDEMAND) end,
      fun q_free/1,      
      [
         {"lease/release",   fun q_lease_release/0}
        ,{"out-of-capacity", fun q_ooc/0}
        ,{"lease timeout",   fun q_lease_timeout/0}
        ,{"suspend/resume",  fun q_suspend_resume/0}
      ]
   }.
-endif.

%%%------------------------------------------------------------------
%%%
%%% generic parameterizable test cases
%%%
%%%------------------------------------------------------------------

q_init(Spec) ->
   _       = application:start(pq),
   {ok, _} = pq:create(tq, Spec).

q_free(_) -> 
   pq:close(tq),
   ok.


q_lease_release() ->
   error_logger:error_msg("~n~n[~p] lease/release", [tq]),
   lease_release(tq, 10),
   erlang:yield().

q_ooc() ->
   error_logger:error_msg("~n~n[~p] out-of-capacity", [tq]),
   spawn(fun() -> lease_release(tq, 1000) end),
   spawn(fun() -> lease_release(tq, 1000) end),
   timer:sleep(100),
   lease_release(tq, 10),
   timer:sleep(1000). %% ensure that queue is empty

q_lease_timeout() ->
   error_logger:error_msg("~n~n[~p] lease timeout", [tq]),
   spawn(fun() -> lease_release(tq, 1000) end),
   spawn(fun() -> lease_release(tq, 1000) end),
   timer:sleep(100),
   {'EXIT', _} = (catch lease_release(tq, 10, 100)),
   timer:sleep(1000). %% ensure that queue is empty

q_suspend_resume() ->
   error_logger:error_msg("~n~n[~p] suspend/resume", [tq]),
   spawn(fun() -> suspend_resume(tq, 1000) end),
   timer:sleep(100),
   lease_release(tq, 10),
   timer:sleep(1200). %% ensure that queue is empty



%%
%%
lease_release(Q, Time) ->
   lease_release(Q, Time, infinity).

lease_release(Q, Time, Timeout) ->
   {ok, Pid} = pq:lease(Q, Timeout),
   ping(Pid, ping),
   timer:sleep(Time),
   ping(Pid, exit),
   ok = pq:release(Q, Pid).

%%
%%
suspend_resume(Q, Time) ->
   ok = pq:suspend(Q),
   timer:sleep(Time),
   ok = pq:resume(Q).


%%
%% ping worker echo process
ping(Pid, Msg) ->
   Pid ! {self(), Msg},
   receive
      Msg -> ok
   end.
