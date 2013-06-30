%%
%% Erlang Redis client
%%
%% Usage:
%%   {ok, Client} = eredis:start_link().
%%   {ok, <<"OK">>} = eredis:q(["SET", "foo", "bar"]).
%%   {ok, <<"bar">>} = eredis:q(["GET", "foo"]).

-module(eredis).
-author('knut.nesheim@wooga.com').

-include("eredis.hrl").

%% Default timeout for calls to the client gen_server
%% Specified in http://www.erlang.org/doc/man/gen_server.html#call-3
-define(TIMEOUT, 5000).

-export([start_link/0, start_link/1, start_link/2, start_link/3, start_link/4,
         start_link/5, stop/1, q/2, q/3, qp/2, qp/3, q_noreply/2]).

-export([prepare_pipeline/1, run_pipeline/3, run_pipeline/4]).

%% Exported for testing
-export([create_multibulk/1]).



%%
%% PUBLIC API
%%

start_link() ->
    start_link("127.0.0.1", 6379, 0, "").

start_link(Host, Port) ->
    start_link(Host, Port, 0, "").

start_link(Host, Port, Database) ->
    start_link(Host, Port, Database, "").

start_link(Host, Port,  Database, Password) ->
    start_link(Host, Port, Database, Password, 100).

start_link(Host, Port, Database, Password, ReconnectSleep)
  when is_list(Host),
       is_integer(Port),
       is_integer(Database) orelse Database == undefined,
       is_list(Password),
       is_integer(ReconnectSleep) orelse ReconnectSleep =:= no_reconnect ->

    eredis_client:start_link(Host, Port, Database, Password, ReconnectSleep).


%% @doc: Callback for starting from poolboy
-spec start_link(server_args()) -> {ok, Pid::pid()} | {error, Reason::term()}.
start_link(Args) ->
    Host           = proplists:get_value(host, Args, "127.0.0.1"),
    Port           = proplists:get_value(port, Args, 6379),
    Database       = proplists:get_value(database, Args, 0),
    Password       = proplists:get_value(password, Args, ""),
    ReconnectSleep = proplists:get_value(reconnect_sleep, Args, 100),
    start_link(Host, Port, Database, Password, ReconnectSleep).

stop(Client) ->
    eredis_client:stop(Client).

-spec q(Client::pid(), Command::iolist()) ->
               {ok, return_value()} | {error, Reason::binary() | no_connection}.
%% @doc: Executes the given command in the specified connection. The
%% command must be a valid Redis command and may contain arbitrary
%% data which will be converted to binaries. The returned values will
%% always be binaries.
q(Client, Command) ->
    call(Client, Command, ?TIMEOUT).

q(Client, Command, Timeout) ->
    call(Client, Command, Timeout).


-spec qp(Client::pid(), Pipeline::pipeline()) ->
                [{ok, return_value()} | {error, Reason::binary()}] |
                {error, no_connection}.
%% @doc: Executes the given pipeline (list of commands) in the
%% specified connection. The commands must be valid Redis commands and
%% may contain arbitrary data which will be converted to binaries. The
%% values returned by each command in the pipeline are returned in a list.
qp(Client, Pipeline) ->
    pipeline(Client, Pipeline, ?TIMEOUT).

qp(Client, Pipeline, Timeout) ->
    pipeline(Client, Pipeline, Timeout).

-spec q_noreply(Client::pid(), Command::iolist()) -> ok.
%% @doc
%% @see q/2
%% Executes the command but does not wait for a response and ignores any errors.
q_noreply(Client, Command) ->
    cast(Client, Command).

%%
%% INTERNAL HELPERS
%%

call(Client, Command, Timeout) ->
    Request = {request, create_multibulk(Command)},
    gen_server:call(Client, Request, Timeout).

%% Template is [iolist() | '$'] . '$' are placeholders to be completed
%% in run_pipeline/3
prepare_pipeline(Template) ->
    ArgCount = [<<$*>>, integer_to_list(length(Template)), <<?NL>>],
    PreparedTemplate = {template, ArgCount, 
        lists:map(fun('$') -> '$';
                (CommandArg) -> to_bulk(to_binary(CommandArg))
        end, Template)},
    Expander = fun(ArgsList) ->
            build_pipeline(PreparedTemplate, ArgsList)
    end,
    {pipeline_template, Expander}.


%% ArgList must already be a [[iolist()]]  (there is no explicit convertion to binary)
run_pipeline(Client, Template, ArgsList) ->
	run_pipeline(Client, Template, ArgsList, ?TIMEOUT).
run_pipeline(_Client, _PipelineTemplate, [], _Timeout) ->
    [];
run_pipeline(Client, PipelineTemplate, ArgsList, Timeout) ->
    Request = {run_pipeline,PipelineTemplate, ArgsList},
    gen_server:call(Client, Request, Timeout).

build_pipeline({template, ArgCount, Template}, ArgsList) ->
	lists:map(fun(Args) ->
                [ArgCount, inflate_template(Template, Args)]
		end, ArgsList).

inflate_template([], []) -> [];
inflate_template(['$'|Template], [Val|Args]) ->
    [to_bulk(Val) | inflate_template(Template, Args)];
inflate_template([B|Template], Args) ->
    [B | inflate_template(Template, Args)].


pipeline(_Client, [], _Timeout) ->
    [];
pipeline(Client, Pipeline, Timeout) ->
    Request = {pipeline, [create_multibulk(Command) || Command <- Pipeline]},
    gen_server:call(Client, Request, Timeout).

cast(Client, Command) ->
    Request = {request, create_multibulk(Command)},
    gen_server:cast(Client, Request).

-spec create_multibulk(Args::iolist()) -> Command::iolist().
%% @doc: Creates a multibulk command with all the correct size headers
create_multibulk(Args) ->
    ArgCount = [<<$*>>, integer_to_list(length(Args)), <<?NL>>],
    ArgsBin = lists:map(fun to_bulk/1, lists:map(fun to_binary/1, Args)),

    [ArgCount, ArgsBin].

%% acept iolist()
to_bulk(B) ->
    [<<$$>>, integer_to_list(iolist_size(B)), <<?NL>>, B, <<?NL>>].

%% @doc: Convert given value to binary. Fallbacks to
%% term_to_binary/1. For floats, throws {cannot_store_floats, Float}
%% as we do not want floats to be stored in Redis. Your future self
%% will thank you for this.
to_binary(X) when is_list(X)    -> list_to_binary(X);
to_binary(X) when is_atom(X)    -> list_to_binary(atom_to_list(X));
to_binary(X) when is_binary(X)  -> X;
to_binary(X) when is_integer(X) -> list_to_binary(integer_to_list(X));
to_binary(X) when is_float(X)   -> throw({cannot_store_floats, X});
to_binary(X)                    -> term_to_binary(X).
