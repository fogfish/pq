-module(pq_tests).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_DISPOSABLE,          true).
-define(TEST_DISPOSABLE_ONDEMAND, true).
-define(TEST_REUSABLE,            true).
-define(TEST_REUSABLE_ONDEMAND,   true).

%%
%% disposable q
-ifdef(TEST_DISPOSABLE).
dq_test_() ->
   {
      setup,
      fun dq_init/0,
      fun dq_free/1,
      [
         {"lease/release",   fun() -> q_lease_release(dq) end}
        ,{"out-of-capacity", fun() -> q_ooc(dq) end}
        ,{"lease timeout",   fun() -> q_lease_timeout(dq) end}
        ,{"suspend/resume",  fun() -> q_suspend_resume(dq) end}
      ]
   }.

dq_init() ->
   _         = application:start(pq),
   {ok, Pid} = pq:start_link(dq, [
      {type,   disposable},
      {capacity,        2}, 
      {worker,    pq_echo}
   ]).

dq_free({ok, Pid}) ->
   erlang:unlink(Pid),
   erlang:exit(Pid, shutdown).
-endif.

%%
%% disposable ondemand q
-ifdef(TEST_DISPOSABLE_ONDEMAND).
dq_ondemand_test_() ->
   {
      setup,
      fun dq_ondemand_init/0,
      fun dq_ondemand_free/1,
      [
         {"lease/release",   fun() -> q_lease_release(ddq) end}
        ,{"out-of-capacity", fun() -> q_ooc(ddq) end}
        ,{"lease timeout",   fun() -> q_lease_timeout(ddq) end}
        ,{"suspend/resume",  fun() -> q_suspend_resume(ddq) end}
      ]
   }.

dq_ondemand_init() ->
   _         = application:start(pq),
   {ok, Pid} = pq:start_link(ddq, [
      {type,   disposable},
      {capacity,        2}, 
      {worker,    pq_echo},
      ondemand
   ]).

dq_ondemand_free({ok, Pid}) ->
   erlang:unlink(Pid),
   erlang:exit(Pid, shutdown).
-endif.

%%
%% reusable q
-ifdef(TEST_REUSABLE).
reusable_test_() ->
   {
      setup,
      fun reusable_init/0,
      fun reusable_free/1,
      [
         {"lease/release",   fun() -> q_lease_release(rq) end}
        ,{"out-of-capacity", fun() -> q_ooc(rq) end}
        ,{"lease timeout",   fun() -> q_lease_timeout(rq) end}
        ,{"suspend/resume",  fun() -> q_suspend_resume(rq) end}
      ]
   }.

reusable_init() ->
   _         = application:start(pq),
   {ok, Pid} = pq:start_link(rq, [
      {type,     reusable},
      {capacity,        2}, 
      {worker,    pq_echo}
   ]).

reusable_free({ok, Pid}) ->
   erlang:unlink(Pid),
   erlang:exit(Pid, shutdown).
-endif.

%%
%% reusable ondemand q
-ifdef(TEST_REUSABLE_ONDEMAND).
reusable_ondemand_test_() ->
   {
      setup,
      fun reusable_ondemand_init/0,
      fun reusable_ondemand_free/1,
      [
         {"lease/release",   fun() -> q_lease_release(drq) end}
        ,{"out-of-capacity", fun() -> q_ooc(drq) end}
        ,{"lease timeout",   fun() -> q_lease_timeout(drq) end}
        ,{"suspend/resume",  fun() -> q_suspend_resume(drq) end}
      ]
   }.

reusable_ondemand_init() ->
   _         = application:start(pq),
   {ok, Pid} = pq:start_link(drq, [
      {type,     reusable},
      {capacity,        2}, 
      {worker,    pq_echo},
      ondemand
   ]).

reusable_ondemand_free({ok, Pid}) ->
   erlang:unlink(Pid),
   erlang:exit(Pid, shutdown).
-endif.

%%%------------------------------------------------------------------
%%%
%%% generic parameterizable test cases
%%%
%%%------------------------------------------------------------------

q_lease_release(Q) ->
   error_logger:error_msg("~n~n[~p] lease/release", [Q]),
   lease_release(Q, 10),
   erlang:yield().

q_ooc(Q) ->
   error_logger:error_msg("~n~n[~p] out-of-capacity", [Q]),
   spawn(fun() -> lease_release(Q, 1000) end),
   spawn(fun() -> lease_release(Q, 1000) end),
   timer:sleep(100),
   lease_release(Q, 10),
   timer:sleep(1000). %% ensure that queue is empty

q_lease_timeout(Q) ->
   error_logger:error_msg("~n~n[~p] lease timeout", [Q]),
   spawn(fun() -> lease_release(Q, 1000) end),
   spawn(fun() -> lease_release(Q, 1000) end),
   timer:sleep(100),
   {'EXIT', _} = (catch lease_release(Q, 10, 100)),
   timer:sleep(1000). %% ensure that queue is empty

q_suspend_resume(Q) ->
   error_logger:error_msg("~n~n[~p] suspend/resume", [Q]),
   spawn(fun() -> suspend_resume(Q, 1000) end),
   timer:sleep(100),
   lease_release(Q, 10),
   timer:sleep(1000). %% ensure that queue is empty



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
