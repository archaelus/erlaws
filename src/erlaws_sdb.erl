%%-------------------------------------------------------------------
%% @author Sascha Matzke <sascha.matzke@didolo.com>
%%   [http://www.didolo.com/] 
%% @copyright 2007 Didolo Ltd.
%% @doc This is an client implementation for Amazon's SimpleDB WebService
%% @end
%%%-------------------------------------------------------------------

-module(erlaws_sdb).

%% exports
-export([create_domain/1, delete_domain/1, list_domains/0, list_domains/1,
	 put_attributes/3, delete_item/2, delete_attributes/3,
	 get_attributes/2, get_attributes/3, list_items/1, list_items/2, 
	 query_items/2, query_items/3]).

%% include record definitions
-include_lib("xmerl/include/xmerl.hrl").

-define(AWS_SDB_HOST, "http://sdb.amazonaws.com/").

%% This function creates a new SimpleDB domain. The domain name must be unique among the 
%% domains associated with your AWS Access Key ID. This function might take 10 
%% or more seconds to complete.. 
%%
%% Spec: create_domain(Domain::string()) -> 
%%       {ok, Domain::string()} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}} 
%%
%%       Code::string() -> "InvalidParameterValue" | "MissingParameter" | "NumberDomainsExceeded"
%%
create_domain(Domain) ->
    try genericRequest(erlaws_cred:get(), "CreateDomain", 
		       Domain, "", [], []) of
	{ok, _Body} -> 
	    {ok, Domain}
    catch 
	throw:{error, Descr} -> 
	    {error, Descr}
    end.

%% This function deletes a SimpleDB domain. Any items (and their attributes) in the domain 
%% are deleted as well. This function might take 10 or more seconds to complete.
%% 
%% Spec: delete_domain(Domain::string()) -> 
%%       {ok, Domain::string()} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%       
%%       Code::string() -> "MissingParameter"
%%
delete_domain(Domain) ->
    try genericRequest(erlaws_cred:get(), "DeleteDomain", 
		       Domain, "", [], []) of
	{ok, _Body} -> Domain
    catch
	throw:{error, Descr} ->
	    {error, Descr}
    end.

%% Lists all domains associated with your Access Key ID. 
%% 
%% Spec: list_domains() -> 
%%       {ok, DomainNames::[string()], ""} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}} 
%%
%% See list_domains/1 for a detailed error description
%%
list_domains() ->
    erlaws_sdb:list_domains([]).

%% Lists domains up to the limit set by {max_domains, integer()}.
%% A NextToken is returned if there are more than max_domains domains. 
%% Calling list_domains successive times with the NextToken returns up
%% to max_domains more domain names each time.
%%
%% Spec: list_domains(Options::[{atom, (string() | integer())}]) ->
%%       {ok, DomainNames::[string()], []} |
%%       {ok, DomainNames::[string()], NextToken::string()} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
%%       Options -> [{max_domains, integer()}, {next_token, string()}]
%%
%%       Code::string() -> "InvalidParameterValue" | "InvalidNextToken" | "MissingParameter"
%%
list_domains(Options) when is_list(Options) ->
    try genericRequest(erlaws_cred:get(), "ListDomains", "", "", [], 
				[makeParam(X) || X <- Options]) of
	{ok, Body} ->
	    {XmlDoc, _Rest} = xmerl_scan:string(Body),
	    DomainNodes = xmerl_xpath:string("//ListDomainsResult/DomainName/text()", 
					     XmlDoc),
	    NextToken = case xmerl_xpath:string("//ListDomainsResult/NextToken/text()", 
						XmlDoc) of
			    [] -> "";
			    [#xmlText{value=NT}|_] -> NT
			end,
	    {ok, [Node#xmlText.value || Node <- DomainNodes], NextToken}
    catch
	throw:{error, Descr} ->
	    {error, Descr}
    end.
    
%% This function creates or replaces attributes in an item. You specify new 
%% attributes using a list of tuples. Attributes are uniquely identified in 
%% an item by their name/value combination. For example, a single item can 
%% have the attributes { "first_name", "first_value" } and { "first_name", 
%% second_value" }. However, it cannot have two attribute instances where 
%% both the attribute name and value are the same.
%%
%% Optionally, you can supply the Replace parameter for each individual 
%% attribute. Setting this value to true causes the new attribute value 
%% to replace the existing attribute value(s). For example, if an item has 
%% the attributes { "a", ["1"] }, { "b", ["2","3"]} and you call this function 
%% using the attributes { "b", "4", true }, the final attributes of the item 
%% are changed to { "a", ["1"] } and { "b", ["4"] }, which replaces the previous 
%% values of the "b" attribute with the new value.
%%
%% Using this function to replace attribute values that do not exist will not 
%% result in an error.
%%
%% The following limitations are enforced for this operation:
%% - 100 attributes per each call
%% - 256 total attribute name-value pairs per item
%% - 250 million attributes per domain
%% - 10 GB of total user data storage per domain
%%
%% Spec: put_attributes(Domain::string(), Item::string(), 
%%                      Attributes::[{Name::string(), (Value::string() | Values:[string()])}]) |
%%       put_attributes(Domain::string(), Item::string(), 
%%                      Attributes::[{Name::string(), (Value::string() | Values:[string()]), 
%%                                    Replace -> true}]) ->
%%       {ok} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
%%       Code::string() -> "InvalidParameterValue" | "MissingParameter" | "NoSuchDomain" |
%%                         "NumberItemAttributesExceeded" | "NumberDomainAttributesExceeded" |
%%                         "NumberDomainBytesExceeded"
%%
put_attributes(Domain, Item, Attributes) when is_list(Domain),
					      is_list(Item),
					      is_list(Attributes) ->
    try genericRequest(erlaws_cred:get(), "PutAttributes", Domain, Item, 
		   Attributes, []) of
	{ok, _Body} -> {ok}
    catch 
	throw:{error, Descr} ->
	    {error, Descr}
    end.

%% Deletes one or more attributes associated with the item. 
%% 
%% Spec: delete_attributes(Domain::string(), Item::string, Attributes::[string()]) ->
%%       {ok} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
%%       Code::string() -> "InvalidParameterValue" | "MissingParameter" | "NoSuchDomain"
%%
delete_attributes(Domain, Item, Attributes) when is_list(Domain),
						 is_list(Item),
						 is_list(Attributes) ->
    try genericRequest(erlaws_cred:get(), "DeleteAttributes", Domain, Item,
		   Attributes, []) of
	{ok, _Body} -> {ok}
    catch 
	throw:{error, Descr} ->
	    {error, Descr}
    end.

%% Deletes the specified item. 
%% 
%% Spec: delete_item(Domain::string(), Item::string()) ->
%%       {ok} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
%%       Code::string() -> "InvalidParameterValue" | "MissingParameter" | "NoSuchDomain"
%%
delete_item(Domain, Item) when is_list(Domain), 
			       is_list(Item) ->
    try delete_attributes(Domain, Item, []) of
	{ok} -> {ok}
    catch
	throw:{error, Descr} ->
	    {error, Descr}
    end.

%% Returns all of the attributes associated with the item.
%%
%% If the item does not exist on the replica that was accessed for this 
%% operation, an empty set is returned. The system does not return an 
%% error as it cannot guarantee the item does not exist on other replicas.
%%
%% Note: Currently SimpleDB is only capable of returning the attributes for
%%       a single item. To be compatible with a possible future this function
%%       returns a list of {Item, Attributes::[{Name::string(), Values::[string()]}]}
%%       tuples. For the time being this list has exactly one member.
%%
%% Spec: get_attributes(Domain::string(), Item::string()) -> 
%%       {ok, Items::[{Item, Attributes::[{Name::string(), Values::[string()]}]}]} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
%%       Code::string() -> "InvalidParameterValue" | "MissingParameter" | "NoSuchDomain"
%%
get_attributes(Domain, Item) when is_list(Domain),
				  is_list(Item) ->
    get_attributes(Domain, Item, "").

%% Returns the requested attribute for an item. 
%%
%% See get_attributes/2 for further documentation.
%%
%% Spec: get_attributes(Domain::string(), Item::string(), Attribute::string()) -> 
%%       {ok, Items::[{Item, Attribute::[{Name::string(), Values::[string()]}]}]} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
%%       Code::string() -> "InvalidParameterValue" | "MissingParameter" | "NoSuchDomain"
%%
get_attributes(Domain, Item, Attribute) when is_list(Domain),
					      is_list(Item),
					      is_list(Attribute) ->
    try genericRequest(erlaws_cred:get(), "GetAttributes", Domain, Item,
		       Attribute, []) of
	{ok, Body} ->
	    {XmlDoc, _Rest} = xmerl_scan:string(Body),
	    AttrList = [{KN, VN} || Node <- xmerl_xpath:string("//Attribute", XmlDoc),
				    begin
					[#xmlText{value=KN}|_] = 
					    xmerl_xpath:string("./Name/text()", Node),
					[#xmlText{value=VN}|_] = 
					    xmerl_xpath:string("./Value/text()", Node),
					true
				    end],
	    {ok, [{Item, lists:foldr(fun aggregateAttr/2, [], AttrList)}]}
    catch 
	throw:{error, Descr} ->
	    {error, Descr}
    end.


%% Returns a list of all items of a domain - 100 at a time. If your
%% domains contains more then 100 item you must use list_items/2 to
%% retrieve all items.
%%
%% Spec: list_items(Domain::string()) ->
%%       {ok, Items::[string()], []} |
%%       {ok, Items::[string()], NextToken::string()} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
%%       Code::string() -> "InvalidParameterValue" | "InvalidNextToken" | 
%%                         "MissingParameter" | "NoSuchDomain"
%%  
list_items(Domain) ->
    list_items(Domain, []).


%% Returns up to max_items -> integer() <= 250 items of a domain. If
%% the total item count exceeds max_items you must call this function
%% again with the NextToken value provided in the return value.
%%
%% Spec: list_items(Domain::string(), Options::[{atom(), (integer() | string())}]) ->
%%       {ok, Items::[string()], []} |
%%       {ok, Items::[string()], NextToken::string()} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
%%       Options -> [{max_items, integer()}, {next_token, string()}]
%% 
%%       Code::string() -> "InvalidParameterValue" | "InvalidNextToken" | 
%%                         "MissingParameter" | "NoSuchDomain"
%%  
list_items(Domain, Options) when is_list(Options) ->
    try genericRequest(erlaws_cred:get(), "Query", Domain, "", [], 
		       [makeParam(X) || X <- Options]) of
	{ok, Body} ->
	    {XmlDoc, _Rest} = xmerl_scan:string(Body),
	    ItemNodes = xmerl_xpath:string("//ItemName/text()", XmlDoc),
	    NextToken = case xmerl_xpath:string("//NextToken/text()", XmlDoc) of
			    [] -> "";
			    [#xmlText{value=NT}|_] -> NT
			end,
	    {ok, [Node#xmlText.value || Node <- ItemNodes], NextToken}
    catch
	throw:{error, Descr} ->
	    {error, Descr}
    end.

%% Executes the given query expression against a domain. The syntax for
%% such a query spec is documented here:
%% http://docs.amazonwebservices.com/AmazonSimpleDB/2007-11-07/DeveloperGuide/SDB_API_Query.html
%%
%% Spec: query_items(Domain::string(), QueryExp::string()]) ->
%%       {ok, Items::[string()], []} |
%%       {ok, Items::[string()], NextToken::string()} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
%%       Code::string() -> "InvalidParameterValue" | "InvalidNextToken" | 
%%                         "MissingParameter" | "NoSuchDomain"
%%  
query_items(Domain, QueryExp) ->
    query_items(Domain, QueryExp, []).

%% Executes the given query expression against a domain. The syntax for
%% such a query spec is documented here:
%% http://docs.amazonwebservices.com/AmazonSimpleDB/2007-11-07/DeveloperGuide/SDB_API_Query.html
%%
%% Spec: list_items(Domain::string(), QueryExp::string(), 
%%                  Options::[{atom(), (integer() | string())}]) ->
%%       {ok, Items::[string()], []} |
%%       {ok, Items::[string()], NextToken::string()} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
%%       Options -> [{max_items, integer()}, {next_token, string()}]
%% 
%%       Code::string() -> "InvalidParameterValue" | "InvalidNextToken" | 
%%                         "MissingParameter" | "NoSuchDomain"
%%  
query_items(Domain, QueryExp, Options) when is_list(Options) ->
    {ok, Body} = genericRequest(erlaws_cred:get(), "Query", Domain, "", [], 
				[{"QueryExpression", QueryExp}|
				 [makeParam(X) || X <- Options]]),
    {XmlDoc, _Rest} = xmerl_scan:string(Body),
    ItemNodes = xmerl_xpath:string("//ItemName/text()", XmlDoc),
    {ok, [Node#xmlText.value || Node <- ItemNodes]}.


%% internal functions

sign (Key,Data) ->
    %io:format("StringToSign:~n ~p~n", [Data]),
    binary_to_list( base64:encode( crypto:sha_mac(Key,Data) ) ).

genericRequest({AccessKey, SecretKey}, Action, Domain, Item, 
	       Attributes, Options) ->
    Timestamp = lists:flatten(erlaws_util:get_timestamp()),
    Signature = sign(SecretKey, Action++Timestamp),
    ActionQueryParams = getQueryParams(Action, Domain, Item, Attributes, 
				       Options),
    FinalQueryParams = [{"AWSAccessKeyId", AccessKey},
			{"Action", Action}, 
			{"Version", "2007-11-07"},
			{"Timestamp", Timestamp},
			{"Signature", Signature}] ++ ActionQueryParams,
    Result = mkReq(FinalQueryParams),
    case Result of
	{ok, _Status, Body} ->
	    {ok, Body};
	{error, {_Proto, 400, _Reason}, Body} ->
	    throw({error, mkErr(Body)});
	{error, {_Proto, Code, Reason}, _Body} ->
	    throw({error, {integer_to_list(Code), Reason, ""}})
    end.

getQueryParams("CreateDomain", Domain, _Item, _Attributes, _Options) ->
    [{"DomainName", Domain}];
getQueryParams("DeleteDomain", Domain, _Item, _Attributes, _Options) ->
    [{"DomainName", Domain}];
getQueryParams("ListDomains", _Domain, _Item, _Attributes, Options) ->
    Options;
getQueryParams("PutAttributes", Domain, Item, Attributes, _Options) ->
    [{"DomainName", Domain}, {"ItemName", Item}] ++
	buildAttributeParams(Attributes);
getQueryParams("GetAttributes", Domain, Item, Attribute, _Options)  ->
    [{"DomainName", Domain}, {"ItemName", Item}] ++
	if length(Attribute) > 0 ->
		[{"AttributeName", Attribute}];
	   true -> []
	end;
getQueryParams("DeleteAttributes", Domain, Item, Attributes, _Options) ->
    [{"DomainName", Domain}, {"ItemName", Item}] ++
	if length(Attributes) > 0 -> 
		buildAttributeParams(Attributes);
	   true -> []
	end;
getQueryParams("Query", Domain, _Item, _Attributes, Options) ->
    [{"DomainName", Domain}] ++ Options.

mkReq(QueryParams) ->
    %io:format("QueryParams:~n ~p~n", [QueryParams]),
    Url = ?AWS_SDB_HOST ++ erlaws_util:queryParams( QueryParams ),
    %io:format("RequestUrl:~n ~p~n", [Url]),
    Request = {Url, []},
    HttpOptions = [{autoredirect, true}],
    Options = [ {sync,true}, {headers_as_is,true}, {body_format, binary} ],
    {ok, {Status, _ReplyHeaders, Body}} = 
	http:request(get, Request, HttpOptions, Options),
    %io:format("Response:~n ~p~n", [binary_to_list(Body)]),
    case Status of 
	{_, 200, _} -> {ok, Status, binary_to_list(Body)};
	{_, _, _} -> {error, Status, binary_to_list(Body)}
    end.

buildAttributeParams(Attributes) ->
    CAttr = collapse(Attributes),
    {_C, L} = lists:foldl(fun flattenParams/2, {0, []}, CAttr),
    %io:format("FlattenedList:~n ~p~n", [L]),
    lists:reverse(L).

mkEntryName(Counter, Key) ->
    {"Attribute." ++ integer_to_list(Counter)
     ++ ".Name", erlaws_util:url_encode(Key)}.
mkEntryValue(Counter, Value) ->
    {"Attribute."++integer_to_list(Counter) ++ ".Value", Value}.

flattenParams({K, V, R}, {C, L}) ->
    PreResult = if R ->
			{C, [{"Attribute." ++ integer_to_list(C) 
			      ++ ".Replace", "true"} | L]};
		   true -> {C, L}
		end,
    FlattenVal = fun(Val, {Counter, ResultList}) ->
			 %io:format("~p -> ~p ~n", [K, Val]),
			 NextCounter = Counter + 1,
			 EntryName = mkEntryName(Counter, K),
			 EntryValue = mkEntryValue(Counter, Val),
			 {NextCounter,  [EntryValue | [EntryName | ResultList]]}
		 end,
    if length(V) > 0 ->
	    lists:foldl(FlattenVal, PreResult, V);
       length(V) =:= 0 -> {C + 1, [mkEntryName(C, K) | L]}
    end.

aggrV({K,V,true}, [{K,L,_OR}|T]) when is_list(V),
				      is_list(hd(V)) -> 
    [{K,V ++ L, true}|T];
aggrV({K,V,true}, [{K,L,_OR}|T]) -> [{K,[V|L], true}|T];

aggrV({K,V,false}, [{K, L, OR}|T]) when is_list(V),
					is_list(hd(V)) -> 
    [{K, V ++ L, OR}|T];
aggrV({K,V,false}, [{K, L, OR}|T]) -> [{K, [V|L], OR}|T];

aggrV({K,V}, [{K,L,OR}|T]) when is_list(V),
				is_list(hd(V))-> 
    [{K,V ++ L,OR}|T];
aggrV({K,V}, [{K,L,OR}|T]) -> [{K,[V|L],OR}|T];

aggrV({K,V,R}, L) when is_list(V),
		       is_list(hd(V)) -> [{K, V, R}|L];
aggrV({K,V,R}, L) -> [{K,[V], R}|L];

aggrV({K,V}, L) when is_list(V),
		     is_list(hd(V)) -> [{K,V,false}|L];
aggrV({K,V}, L) -> [{K,[V],false}|L];

aggrV({K}, L) -> [{K, [], false}|L].

collapse(L) ->
    AggrL = lists:foldl( fun aggrV/2, [], lists:keysort(1, L) ),
    lists:keymap( fun lists:sort/1, 2, lists:reverse(AggrL)).

makeParam(X) ->
    case X of
	{_, []} -> {};
	{max_items, MaxItems} when is_integer(MaxItems) ->
	    {"MaxNumberOfItems", integer_to_list(MaxItems)};
	{max_domains, MaxDomains} when is_integer(MaxDomains) ->
	    {"MaxNumberOfDomains", integer_to_list(MaxDomains)};
	{next_token, NextToken} ->
	    {"NextToken", NextToken};
	_ -> {}
    end.


aggregateAttr ({K,V}, [{K,L}|T]) -> [{K,[V|L]}|T];
aggregateAttr ({K,V}, L) -> [{K,[V]}|L].

mkErr(Xml) ->
    {XmlDoc, _Rest} = xmerl_scan:string( Xml ),
    [#xmlText{value=ErrorCode}|_] = xmerl_xpath:string("//Error/Code/text()", XmlDoc),
    [#xmlText{value=ErrorMessage}|_] = xmerl_xpath:string("//Error/Message/text()", XmlDoc),
    [#xmlText{value=RequestId}|_] = xmerl_xpath:string("//RequestID/text()", XmlDoc),
    {ErrorCode, ErrorMessage, RequestId}.
