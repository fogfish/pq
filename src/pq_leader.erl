%% @description
%%   pq leader, allocates requests among workers 
-module(pq_leader).
-behaviour(gen_server).
-include("pq.hrl").

-export([
   %% api
   start_link/3, 
   % gen_server
   init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3
]).

-record(leader, {
   type             :: disposable | reusable,
   factory          :: pid(),     % worker factory
   capacity = 0     :: integer(), % queue capacity (remaining workers)
   size     = 0     :: integer(), % queue size     (max number of workers)
   inactive = false :: boolean(), % queue is suspended
   lq,              % lease queue
   wq,              % worker queue
   ondemand = false % one-demand worker allocation is used
}).

%%
%%
start_link(Sup, undefined, Opts) ->
   gen_server:start_link(?MODULE, [Sup, Opts], []);

start_link(Sup, Name, Opts) ->
   gen_server:start_link({local, Name}, ?MODULE, [Sup, Opts], []).

init([Sup, Opts]) ->
   self() ! {set_factory, Sup},
   {ok, init(Opts, #leader{})}.

init([{capacity, X} | Opts], S)
 when is_integer(X) ->
   init(Opts, S#leader{capacity=X, size=X});

init([{type, X} | Opts], S) ->
   init(Opts, S#leader{type=X});

init([ondemand | T], S) ->
   init(T, S#leader{ondemand=true});

init([{register, Fun} | Opts], S)
 when is_function(Fun) ->
   % register function allow to implement custom queue identity schema
   Fun(),
   init(Opts, S);

init([_ | Opts], S) ->
   init(Opts, S);

init([], S) ->
   S#leader{
      lq = q:new(),
      wq = q:new()
   }.

terminate(_, _) ->
   ok.

%%%------------------------------------------------------------------
%%%
%%% gen_server
%%%
%%%------------------------------------------------------------------

%%
%%
handle_call({lease, Timeout}, Tx, #leader{capacity=0}=S) ->
   {noreply, enq_request(Timeout, Tx, S)};

handle_call({lease, Timeout}, Tx, #leader{inactive=true}=S) ->
   {noreply, enq_request(Timeout, Tx, S)};

handle_call({lease, Timeout}, Tx, S) ->
   {noreply, deq_worker(enq_request(Timeout, Tx, S))};


handle_call({release, _Pid}, Tx, #leader{inactive=true}=S) ->
   % TODO: shutdown Pid
   gen_server:reply(Tx, ok), % reply immediately
   {noreply, S};

handle_call({release, _Pid}, Tx, #leader{type=disposable}=S) ->
   % no needs to kill worker - it kill itself
   % no needs to increase capacity, death of worker made it
   % we have to wait for worker termination so that 
   % it is capable to perform graceful exit
   gen_server:reply(Tx, ok),
   {noreply, S};

handle_call({release, Pid}, Tx, #leader{type=reusable}=S) ->
   % no needs to kill worker - it is reusable
   gen_server:reply(Tx, ok),
   case is_process_alive(Pid) of
      true  -> 
         {noreply, deq_worker(enq_worker(Pid, S))};
      % no needs to increase capacity, death of worker made it   
      false ->
         {noreply, S}
   end;

handle_call(suspend, _, S) ->
   ?DEBUG("pq ~p suspend", [self()]),
   free_worker(S#leader.wq),
   {reply, ok, 
      S#leader{
         inactive = true,
         capacity = S#leader.size,
         wq       = q:new()
      }
   };

handle_call(resume, _, #leader{inactive=false}=S) ->
   {reply, ok, S};

handle_call(resume, _, #leader{ondemand=false}=S) ->
   ?DEBUG("pq ~p resume", [self()]),
   Workers = [init_worker(S#leader.factory) || _ <- lists:seq(1, S#leader.capacity)],
   {reply, ok,
      deq_worker(
         S#leader{
            inactive = false,
            wq       = lists:foldl(fun q:enq/2, q:new(), Workers)
         }
      )
   };

handle_call(resume, _, S) ->
   {reply, ok, 
      deq_worker(
         S#leader{
            inactive = false
         }
      )
   };

handle_call(_, _, S) ->
   {noreply, S}.

%%
%%
handle_cast(_, S) ->
   {noreply, S}.

%%
%%
handle_info({set_factory, Sup}, #leader{ondemand=false}=S) ->
   {ok, Pid} = pq_queue_sup:worker(Sup),
   Workers   = [init_worker(Pid) || _ <- lists:seq(1, S#leader.capacity)],
   {noreply, 
      S#leader{
         factory = Pid,
         wq      = lists:foldl(fun q:enq/2, q:new(), Workers)
      }
   };

handle_info({set_factory, Sup}, S) ->
   {ok, Pid} = pq_queue_sup:worker(Sup),
   {noreply, S#leader{factory=Pid}};

handle_info({'DOWN', _, _, _Pid, _Reason}, #leader{inactive=true}=S) ->
   % queue is not active, do not recover worker
   {noreply, S};

handle_info({'DOWN', _, _, Pid, Reason}, #leader{ondemand=false}=S) ->
   % loss of worker is (release empty), we have to decrease capacity
   % to ensure that new worker is restarted  
   ?DEBUG("pq ~p die worker: ~p, reason ~p", [self(), Pid, Reason]),
   {noreply, 
      deq_worker(
         enq_worker(
            dec_capacity(S)
         )
      )
   };

handle_info({'DOWN', _, _, Pid, Reason}, S) ->
   ?DEBUG("pq ~p die worker: ~p, reason ~p", [self(), Pid, Reason]),
   {noreply, 
      deq_worker(
         inc_capacity(S)
      )
   };

%%
%% plib interface
handle_info({call, Tx, Msg}, #leader{capacity=0}=S) ->
   {noreply, enq_plib_request(Tx, Msg, S)};

handle_info({call, Tx, Msg}, #leader{inactive=true}=S) ->
   {noreply, enq_plib_request(Tx, Msg, S)};

handle_info({call, Tx, Msg}, S) ->
   {noreply, deq_worker(enq_plib_request(Tx, Msg, S))};

handle_info({cast, Tx, Msg}, #leader{capacity=0}=S) ->
   {noreply, enq_plib_request(Tx, Msg, S)};

handle_info({cast, Tx, Msg}, #leader{inactive=true}=S) ->
   {noreply, enq_plib_request(Tx, Msg, S)};

handle_info({cast, Tx, Msg}, S) ->
   {noreply, deq_worker(enq_plib_request(Tx, Msg, S))};

handle_info(_, S) ->
   %error_logger:error_msg("-- msg --> ~p", [M]),
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

%%
%% enqueue lease request
enq_request(Timeout, Req, S) ->
   ?DEBUG("pq ~p (c=~b) enq request: ~p (timeout ~p)", [self(), S#leader.capacity, Req, Timeout]),
   S#leader{
      lq = q:dropwhile( 
         fun pq_util:expired/1, 
         q:enq({lease, pq_util:deadline(Timeout), Req}, S#leader.lq)
      )
   }.

%%
%% enqueue plib request
enq_plib_request(Tx, Msg, S) ->
   ?DEBUG("pq ~p (c=~b) enq plib: ~p", [self(), S#leader.capacity, Tx]),
   S#leader{
      lq = q:dropwhile( 
         fun pq_util:expired/1, 
         q:enq({plib, Tx, Msg}, S#leader.lq)
      )
   }.

%%
%% enqueue new worker
enq_worker(#leader{capacity=C, size=Size}=S)
 when C =< Size ->
   % limit number of workers
   enq_worker(init_worker(S#leader.factory), S);

enq_worker(S) ->
   S.

enq_worker(Worker, S) ->
   ?DEBUG("pq ~p (c=~b) enq worker: ~p", [self(), S#leader.capacity, Worker]),
   inc_capacity(
      S#leader{
         wq = q:enq(Worker, S#leader.wq)
      }
   ).

%%
%%
deq_worker(#leader{lq={}}=S) ->
   % no valid lease request
   ?DEBUG("pq ~p (c=~b) deq worker: no lease", [self(), S#leader.capacity]),
   S;

deq_worker(#leader{wq={}}=S) ->
   % no valid worker
   Worker = init_worker(S#leader.factory),
   Lq = case q:deq(S#leader.lq) of
      {{lease, _, Req}, Q} ->
         gen_server:reply(Req, {ok, Worker}),
         Q;
      {{plib, Tx, Msg}, Q} ->
         plib:relay(Worker, Tx, Msg),
         Q
   end,
   ?DEBUG("pq ~p (c=~b) deq worker: ~p to ~p", [self(), S#leader.capacity, Worker, Req]),
   S#leader{
      lq       = Lq,
      capacity = S#leader.capacity - 1
   }; 

deq_worker(S) ->
   {Worker,   Wq} = q:deq(S#leader.wq),
   case is_process_alive(Worker) of
      false ->
         deq_worker(S#leader{wq=Wq});
      true  ->
         Lq = case q:deq(S#leader.lq) of
            {{lease, _, Req}, Q} ->
               gen_server:reply(Req, {ok, Worker}),
               Q;
            {{plib, Tx, Msg}, Q} ->
               plib:relay(Worker, Tx, Msg),
               Q
         end,
         ?DEBUG("pq ~p (c=~b) deq worker: ~p to ~p", [self(), S#leader.capacity, Worker, Req]),
         S#leader{
            lq       = Lq,
            wq       = Wq,
            capacity = S#leader.capacity - 1
         }
   end.

%%
%% increase / decrease capacity 
inc_capacity(#leader{}=S) ->
   S#leader{
      capacity = erlang:min(S#leader.capacity + 1, S#leader.size)
   }.   

dec_capacity(#leader{}=S) ->
   S#leader{
      capacity = erlang:max(S#leader.capacity - 1, 0)
   }.   


%%
%%
init_worker(Sup) ->
   {ok, Pid} = supervisor:start_child(Sup, []),
   erlang:monitor(process, Pid),
   ?DEBUG("pq ~p init worker: ~p", [self(), Pid]),
   Pid.

%%
%%
free_worker({}) ->
   ok;
free_worker(Q) ->
   Pid = q:hd(Q),
   ?DEBUG("pq ~p free worker: ~p", [self(), Pid]),
   erlang:exit(Pid, shutdown),
   free_worker(q:tl(Q)).

