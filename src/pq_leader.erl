-module(pq_leader).
-behaviour(gen_server).

-export([
   %% api
   start_link/3, 
   lease/2, release/2,
   suspend/1, resume/1,
   % gen_server
   init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3
]).

-record(srv, {
   sup,           % parent supervisor
   worker,        % worker factory
   capacity =  0, % queue capacity
   length   = 10, % queue length
   q,             % worker queue
   wait,          % client queue waiting for worker

   inactive = false,  % queue is suspended
   reusable = false,  % workers are re-usable, release operation do not evict them
   ondemand = false,  % workers are spawned on-demand
   throttle = false   % throttle is off, lease operation do to recover consumed capacity
}).

%%
%%
start_link(Sup, undefined, Opts) ->
   gen_server:start_link(?MODULE, [Sup, Opts], []);

start_link(Sup, Q, Opts) ->
   gen_server:start_link({local, Q}, ?MODULE, [Sup, Opts], []).

init([Sup, Opts]) ->
   init(Opts, #srv{sup=Sup}).

init([{length, X} | T], S) when is_integer(X) ->
   init(T, S#srv{length=X});

init([reusable | T], S) ->
   init(T, S#srv{reusable=true});

init([ondemand | T], S) ->
   init(T, S#srv{ondemand=true});

init([throttle | T], S) ->
   init(T, S#srv{throttle=true});

init([_ | T], S) ->
   init(T, S);

init([], S) ->
   {ok, init_empty_q(S), 0}.

terminate(_, _) ->
   ok.

%%
init_empty_q(#srv{}=S) ->
   S#srv{
      q    = queue:new(),
      wait = queue:new()
   }.


%%%------------------------------------------------------------------
%%%
%%% api
%%%
%%%------------------------------------------------------------------

%%
%%
-spec(lease/2 :: (atom(), integer() | infinity) -> {ok, pid()} | {error, any()}).

lease(Q, Timeout) ->
   try
      gen_server:call(Q, {lease, Timeout}, Timeout)
   catch 
      exit:{timeout, _} -> {error, timeout}
   end.
   
%%
%%
-spec(release/2 :: (atom(), pid()) -> ok | {error, any()}).

release(Q, Pid) ->
   gen_server:call(Q, {release, Pid}).

%%
%%
suspend(Q) ->
   gen_server:call(Q, suspend).

%%
%%
resume(Q) ->
   gen_server:call(Q, resume).

%%%------------------------------------------------------------------
%%%
%%% gen_server
%%%
%%%------------------------------------------------------------------

%%
%%
handle_call({lease, Timeout}, Tx0, #srv{inactive=false}=S0) ->
   S1        = push_lease_request(Tx0, Timeout, S0),
   {Tx1, S2} = peek_lease_request(S1),
   case lease_worker(Tx1, S2) of
      false -> {noreply, S1};
      S3    -> {noreply, S3}
   end;

handle_call({lease, Timeout}, Tx0, #srv{inactive=true}=S0) ->
   {noreply, push_lease_request(Tx0, Timeout, S0)};

handle_call({release, Pid}, Tx, #srv{inactive=false}=S0) ->
   gen_server:reply(Tx, ok),
   {noreply, release_worker(Pid, S0)};

handle_call({release, Pid}, Tx, #srv{inactive=true}=S0) ->
   gen_server:reply(Tx, ok),
   {noreply, free_worker(Pid, S0)};

handle_call(suspend, _, #srv{inactive=true}=S) ->
   {reply, ok, S};

handle_call(suspend, _, #srv{inactive=false}=S) ->
   {reply, ok, free_worker_q(S#srv{inactive=true})};

handle_call(resume, _, #srv{inactive=true}=S) ->
   {reply, ok, init_worker_q(S#srv{inactive=false})};

handle_call(resume, _, #srv{inactive=false}=S) ->
   {reply, ok, S};

handle_call(_, _, S) ->
   {noreply, S}.

%%
%%
handle_cast(_, S) ->
   {noreply, S}.

%%
%%
handle_info(timeout, #srv{sup=Sup}=S) ->
   {ok, Pid} = pq_q_sup:worker(Sup),
   {noreply, init_worker_q(S#srv{worker=Pid})};

handle_info({'DOWN', _, _, _Pid, _}, #srv{capacity=C, ondemand=false}=S) ->
   % one of our workers is dead
   % do nothing to filter it our but decrease capacity
   {noreply, init_worker(S#srv{capacity=C - 1})};

handle_info({'DOWN', _, _, _Pid, _}, #srv{capacity=C, ondemand=true}=S) ->
   % one of our workers is dead
   % do nothing to filter it our but decrease capacity
   {noreply, S#srv{capacity=C - 1}};

handle_info(_, S) ->
   {noreply, S}.

%%
%% 
code_change(_Vsn, S, _Extra) ->
   {ok, S}.


%%%------------------------------------------------------------------
%%%
%%% private
%%%
%%%------------------------------------------------------------------

%% init queue of workers
init_worker_q(#srv{length=Len,  ondemand=false}=S0) ->
   lists:foldl(fun(_, S) -> init_worker(S) end, S0, lists:seq(1, Len));

init_worker_q(#srv{length=_Len, ondemand=true}=S0) ->
   S0.

init_worker(#srv{worker=Sup, capacity=C, q=Q}=S) ->
   {ok, Pid} = supervisor:start_child(Sup, []),
   erlang:monitor(process, Pid),
   S#srv{
      q        = queue:in(Pid, Q),
      capacity = C + 1
   }.

free_worker_q(#srv{q=Q}=S) ->
   free_worker_q(queue:out(Q), S).

free_worker_q({empty, Q}, S) ->
   S#srv{
      q = Q
   };

free_worker_q({{value, Pid}, Q}, S) ->
   free_worker_q(queue:out(Q), free_worker(Pid, S)).

free_worker(Pid, #srv{capacity=C}=S) ->
   erlang:exit(Pid, shutdown),
   S#srv{
      capacity = C - 1
   }.

%%
%% lease worker
lease_worker(undefined, S) ->
   S;

lease_worker(Tx, #srv{q=Q}=S) ->
   lease_worker(queue:out(Q), Tx, S).

lease_worker({empty, _}, Tx, #srv{capacity=C, length=L}=S)
 when C < L ->
   lease_worker(Tx, init_worker(S));

lease_worker({empty, _}, _Tx, #srv{capacity=C, length=L})
 when C =:= L ->
   false;

lease_worker({{value, Pid}, Q}, Tx, #srv{}=S) ->
   case is_process_alive(Pid) of
      true  -> allocate_worker(Pid, Tx, S#srv{q=Q});
      false -> lease_worker(queue:out(Q), Tx, S)
   end.

allocate_worker(Pid, Tx, #srv{reusable=false, throttle=true, capacity=C}=S) ->
   gen_server:reply(Tx, {ok, Pid}),
   init_worker(S#srv{capacity=C - 1});

allocate_worker(Pid, Tx, #srv{reusable=false, throttle=false, capacity=C}=S) ->
   gen_server:reply(Tx, {ok, Pid}),
   S#srv{capacity=C - 1};

allocate_worker(Pid, Tx, #srv{reusable=true}=S) ->
   gen_server:reply(Tx, {ok, Pid}),
   S.

%%
%% release used worker
release_worker(Pid, #srv{reusable=false}=S0) ->
   erlang:exit(Pid, shutdown),
   {Tx, S1} = peek_lease_request(S0),
   case lease_worker(Tx, S1) of
      false -> S0;  % worker cannot be leased (rollback one state)
      S2    -> S2   % worker is leased
   end;

release_worker(Pid, #srv{q=Q, reusable=true}=S0) ->
   S1 = case is_process_alive(Pid) of
      true   -> S0#srv{q = queue:in(Pid, Q)};
      false  -> S0
   end,
   {Tx, S2} = peek_lease_request(S1),
   case lease_worker(Tx, S2) of
      false -> S1;  % worker cannot be leased (rollback one state)
      S3    -> S3   % worker is leased
   end.

%%
%% 
push_lease_request(Tx, infinity, #srv{wait=Q}=S) ->
   S#srv{
      wait = queue:in({infinity, Tx}, Q)
   };

push_lease_request(Tx, Timeout, #srv{wait=Q}=S) ->
   S#srv{
      wait = queue:in({usec() + Timeout * 1000, Tx}, Q)
   }.

%%
peek_lease_request(#srv{wait=Q}=S) ->
   peek_lease_request(queue:out(Q), S).

peek_lease_request({{value, {Deadline, Tx}}, Q}, #srv{}=S) ->
   case usec() of
      Now when Now < Deadline ->
         {Tx, S#srv{wait = Q}};
      _ ->
         peek_lease_request(queue:out(Q), S)
   end;

peek_lease_request({empty, Q}, #srv{}=S) ->
   {undefined, S#srv{wait = Q}}.

%%
%%
usec() ->
   {Mega, Sec, Micro} = erlang:now(),
   (Mega * 1000000 + Sec) * 1000000 + Micro.
