%%%-------------------------------------------------------------------
%%% File    : s3client.erl
%%% Author  : Sascha Matzke <sascha.matzke@didolo.com>
%%% Description : Amazon S3 client library
%%%
%%% Created : 25 Dec 2007 by Sascha Matzke <sascha.matzke@didolo.com>
%%%-------------------------------------------------------------------

-module(erlaws_cred).

-behaviour(gen_server).

%% API
-export([start/1, stop/0]).
-export([get/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

%%--------------------------------------------------------------------
%% Description: Starts the server
%%
%% start(Credentials) where:
%%     Credentials ::= { AWSAccessKey, AWSSecretKey}
%%
%% Returns: {ok,Pid} | ignore | {error,Error}
%%--------------------------------------------------------------------
start(Credentials) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Credentials, []).

stop() -> 
    gen_server:cast(erlaws_cred, stop). 


%%--------------------------------------------------------------------
%% ...
%%--------------------------------------------------------------------

get() ->
    gen_server:call(?MODULE, {get_creds}).


%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init(Credentials) ->
    {ok, Credentials}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------

handle_call({get_creds}, _From, Credentials) ->
    {reply, Credentials, Credentials}.


%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast(stop, State) -> 
{stop, normal, State}; 

handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

