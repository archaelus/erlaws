-module(erlaws_app). 

-behaviour(application). 

-export([start/2, stop/1]). 

start(_Type, Args) -> 
    erlaws:start(Args). 

stop(_State) -> 
    ok. 
