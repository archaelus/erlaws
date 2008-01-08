-module(erlaws). 

-behaviour(application). 

-export([start/0, start/2, stop/1]). 

start() ->
    application:start(sasl),
    crypto:start(),
    inets:start(),
    application:start(erlaws).

start(_Type, _Args) -> 
    Env = application:get_env(aws_credentials),
    io:format("Creds:~n ~p~n", [Env]),
    {ok, Creds} = Env,
    erlaws_cred:start(Creds). 

stop(_State) -> 
    erlaws_cred:stop(),
    ok. 
