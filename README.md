# Process Queue - Pool of Workers

The library is an implementation of processes pool pattern. It instantiates pool of worker processes, supervises them and leases workers to clients on the request. The library provides primitives to clients to lease / release unit of work objects.

## Configuration

The client spawns pools using ```pq:start_link([...])```. It accepts following options

 * ```{type, disposable | reusable}``` defines the supervision strategy for worker processes. A disposable worker is destroyed once it is released back by client (the new worker is spawned for each lease request). A reusable worker is preserved.

 * ```{capacity, integer()}``` defines max number of worker processes available to lease. This is cardinality of pool.  

 * ```{worker, mfa() | atom() | {atom(), list()}}``` defines worker process. The parameter carries on module, function and arguments are used to spawn a new worker process. 

 * ```{ttl, integer()}``` defines time-to-live in milliseconds for worker process. The worker process is terminated on expired ttl timer. 


## Usage 

```erlang
   %% 
   %% start application and pool instance
	application:start(pq).
	{ok, _} = pq:start_link(my_pool, [{type, reusable}, {capacity, 10}, {worker, pq_echo}]).

   %%
   %% use worker
   Ref = pq:lease(my_pool).
   erlang:send(pq:pid(Ref), ping).
   pq:release(Ref).
``` 
