%% @description
%%   pq leader for async workers
-module(pq_leader_async).
-behaviour(gen_server).

-export([
   %% api
   start_link/3, 
   % gen_server
   init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3
]).

-record(srv, {
   sup,           % parent supervisor
   identity,      % queue identity
   worker,        % worker factory
   length   = 10, % queue length
   pq,            % process queue
   lq,            % lease queue

   lock     = false,  % queue is locked / unlocked
   ondemand = false,  % workers are spawned on-demand
   throttle = false   % throttle is off, lease operation do to recover consumed capacity 
}).

%%
%%
start_link(Sup, undefined, Opts) ->
   gen_server:start_link(?MODULE, [Sup, Opts], []);

start_link(Sup, Name, Opts) ->
   gen_server:start_link({local, Name}, ?MODULE, [Sup, Opts], []).

init([Sup, Opts]) ->
   init(Opts, #srv{sup=Sup}).

init([{length, X} | T], S) when is_integer(X) ->
   init(T, S#srv{length=X});

init([ondemand | T], S) ->
   init(T, S#srv{ondemand=true});

init([throttle | T], S) ->
   init(T, S#srv{throttle=true});

init([{register, Fun} | T], S)
 when is_function(Fun) ->
   % register function allow to implement various process identity schema
   init(T, S#srv{identity=Fun()});

init([_ | T], S) ->
   init(T, S);

init([], S) ->
   {ok, S, 0}.

terminate(_, _) ->
   ok.

%%%------------------------------------------------------------------
%%%
%%% gen_server
%%%
%%%------------------------------------------------------------------

%%
%%
handle_call({lease, Timeout}, Tx, #srv{lock=false}=S) ->
   {noreply, lease(Tx, Timeout, S)};

handle_call({lease, Timeout}, Tx, #srv{lq=Lq}=S) ->
   % queue is suspended
   {noreply, S#srv{lq=pq_lease:in(Tx, Timeout, Lq)}};

handle_call({release, _Pid}, _Tx, S) ->
   % async worker releases queue when it dies.
   % with async worker the life cycle is following
   % lease, cast, realase, .... die
   {reply, ok, S};

handle_call(suspend, _, #srv{lock=false, pq=Pq}=S) ->
   {reply, ok, 
      S#srv{
         lock = true,
         pq   = pq_process:free(Pq)
      }
   };

handle_call(resume, _, #srv{lock=true, ondemand=false, length=N, pq=Pq}=S) ->
   {reply, ok, 
      S#srv{
         lock = false,
         pq   = pq_process:init(N, Pq)
      }
   };

handle_call(resume, _, #srv{lock=true}=S) ->
   {reply, ok, 
      S#srv{
         lock = false
      }
   };

handle_call(_, _, S) ->
   {reply, ok, S}.

%%
%%
handle_cast(_, S) ->
   {noreply, S}.

%%
%%
handle_info(timeout, #srv{sup=Sup, ondemand=false, length=N}=S) ->
   {ok, Pid} = pq_q_sup:worker(Sup),
   {noreply,
      S#srv{
         lq = pq_lease:new(),
         pq = pq_process:init(N, pq_process:new(N, fun() -> spawner(Pid) end))
      }
   };

handle_info(timeout, #srv{sup=Sup, length=N}=S) ->
   {ok, Pid} = pq_q_sup:worker(Sup),
   {noreply,
      S#srv{
         lq = pq_lease:new(),
         pq = pq_process:new(N, fun() -> spawner(Pid) end)
      }
   };


handle_info({'DOWN', _, _, Pid, _}, #srv{ondemand=false, lock=false, pq=Pq}=S) ->
   % one of our workers is dead, do nothing to filter it out but spawn new capacity
   {noreply, S#srv{pq=pq_process:spawn(Pid, Pq)}};

handle_info({'DOWN', _, _, Pid, _}, #srv{ondemand=true, lock=false, pq=Pq}=S) ->
   % one of our workers is dead, do nothing to filter it out but decrease capacity
   {noreply, S#srv{pq=pq_process:evict(Pid, Pq)}};

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

%% handle lease request
lease(Tx, Timeout, #srv{lq=Lq}=S) ->
   Q = pq_lease:in(Tx, Timeout, Lq),
   try
      request_capacity(pq_lease:out(Q), S)
   catch throw:ooc ->
      S#srv{lq=Q}
   end.

%% request capacity
request_capacity({undefined, Lq}, S) ->
   % no valid lease request
   S#srv{lq=Lq}; 

request_capacity({Tx, Lq}, #srv{pq=Pq}=S) ->
   % valid lease request exists, fullfil it
   allocate_capacity(pq_process:out(Pq), Tx, S#srv{lq=Lq}).    

%% allocate capacity to requester
allocate_capacity({undefined, _Pq}, _Tx, _S) ->
   % there is no capacity available
   throw(ooc);

allocate_capacity({Pid, Pq}, Tx, S) ->
   % there is capacity
   gen_server:reply(Tx, {ok, Pid}),
   S#srv{pq=Pq}.

%%
%%
spawner(Sup) ->
   {ok, Pid} = supervisor:start_child(Sup, []),
   _ = erlang:monitor(process, Pid),
   Pid.
