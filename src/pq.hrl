

-define(VERBOSE, true).

-ifdef(VERBOSE).
-define(DEBUG(Str, Args), error_logger:error_msg(Str, Args)).
-else.
-define(DEBUG(Str, Args), ok).
-endif.