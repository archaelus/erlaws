%%-------------------------------------------------------------------
%% @author Sascha Matzke <sascha.matzke@didolo.com>
%%   [http://www.didolo.com/] 
%% @copyright 2007 Didolo Ltd.
%% @doc This is an client implementation for Amazon's Simple Queue Service
%% @end
%%%-------------------------------------------------------------------

-module(erlaws_sqs).

%% exports
-export([list_queues/0, list_queues/1, get_queue/1, create_queue/1,
	 create_queue/2, get_queue_attr/1, set_queue_attr/3, 
	 delete_queue/1, delete_queue/2, send_message/2, 
	 peek_message/2, receive_message/1, receive_message/2,
	 delete_message/2]).

%% include record definitions
-include_lib("xmerl/include/xmerl.hrl").

-define(AWS_SQS_HOST, "queue.amazonaws.com").
-define(AWS_SQS_URL, "http://" ++ ?AWS_SQS_HOST ++ "/").

%% queues

%% Creates a new SQS queue with the given name. 
%%
%% SQS assigns the queue a queue URL; you must use this URL when 
%% performing actions on the queue (for more information, see 
%% http://docs.amazonwebservices.com/AWSSimpleQueueService/2007-05-01/SQSDeveloperGuide/QueueURL.html).
%%
%% Spec: create_queue(QueueName::string()) ->
%%       {ok, QueueUrl::string()} |
%%       {error, {Code::string, Msg::string(), ReqId::string()}}
%% 
create_queue(QueueName) ->
    try query_request(erlaws_cred:get(), ?AWS_SQS_URL, "CreateQueue", 
		       [{"QueueName", QueueName}]) of
	{ok, Body} ->
	    {XmlDoc, _Rest} = xmerl_scan:string(Body),
	    [#xmlText{value=Queue}|_] = 
		xmerl_xpath:string("//QueueUrl/text()", XmlDoc),
	    {ok, Queue}
    catch
	throw:{error, Descr} ->
	    {error, Descr}
    end.

%% Creates a new SQS queue with the given name and default VisibilityTimeout.
%% 
%% Spec: create_queue(QueueName::string(), VisibilityTimeout::integer()) ->
%%       {ok, QueueUrl::string()} |
%%       {error, {Code::string, Msg::string(), ReqId::string()}}
%% 
create_queue(QueueName, VisibilityTimeout) when is_integer(VisibilityTimeout) ->
    try query_request(erlaws_cred:get(), ?AWS_SQS_URL, "CreateQueue", 
		   [{"QueueName", QueueName}, 
		    {"DefaultVisibilityTimeout", 
		     integer_to_list(VisibilityTimeout)}]) of
	{ok, Body} ->
	    {XmlDoc, _Rest} = xmerl_scan:string(Body),
	    [#xmlText{value=QueueUrl}|_] = 
		xmerl_xpath:string("//QueueUrl/text()", XmlDoc),
	    {ok, QueueUrl}
    catch
	throw:{error, Descr} ->
	    {error, Descr}
    end.
	

%% Returns a list of existing queues (QueueUrls).
%%
%% Spec: list_queues() ->
%%       {ok, [QueueUrl::string(),...]} |
%%       {error, {Code::string, Msg::string(), ReqId::string()}}
%%
list_queues() ->
    try query_request(erlaws_cred:get(), ?AWS_SQS_URL, "ListQueues", []) of
	{ok, Body} ->
	    {XmlDoc, _Rest} = xmerl_scan:string(Body),
	    QueueNodes = xmerl_xpath:string("//QueueUrl/text()", XmlDoc),
	    {ok, [QueueUrl || #xmlText{value=QueueUrl} <- QueueNodes]}
    catch
	throw:{error, Descr} ->
	    {error, Descr}
    end.

%% Returns a list of existing queues (QueueUrls) whose names start
%% with the given prefix
%%
%% Spec: list_queues(Prefix::string()) ->
%%       {ok, [QueueUrl::string(),...]} |
%%       {error, {Code::string, Msg::string(), ReqId::string()}}
%%
list_queues(Prefix) ->
    try query_request(erlaws_cred:get(), ?AWS_SQS_URL, "ListQueues",  
		       [{"QueueNamePrefix", Prefix}]) of
	{ok, Body} ->
	    {XmlDoc, _Rest} = xmerl_scan:string(Body),
	    QueueNodes = xmerl_xpath:string("//QueueUrl/text()", XmlDoc),
	    {ok, [Queue || #xmlText{value=Queue} <- QueueNodes]}
    catch
	throw:{error, Descr} ->
	    {error, Descr}
    end.
    
%% Returns the Url for a specific queue-name
%%
%% Spec: get_queue(QueueName::string()) ->
%%       {ok, QueueUrl::string()} |
%%       {error, {Code::string, Msg::string(), ReqId::string()}}
%%
get_queue(QueueName) ->
    try query_request(erlaws_cred:get(), ?AWS_SQS_URL, "ListQueues",  
		       [{"QueueNamePrefix", QueueName}]) of
	{ok, Body} ->
	    {XmlDoc, _Rest} = xmerl_scan:string(Body),
	    QueueNodes = xmerl_xpath:string("//QueueUrl/text()", XmlDoc),
	    [QueueUrl|_] = [Queue || #xmlText{value=Queue} <- QueueNodes],
	    {ok, QueueUrl}
    catch
	throw:{error, Descr} ->
	    {error, Descr}
    end.

%% Returns the attributes for the given QueueUrl
%%
%% Spec: get_queue_attr(QueueUrl::string()) ->
%%       {ok, [{"VisibilityTimeout", Timeout::integer()},
%%             {"ApproximateNumberOfMessages", Number::integer()}]} |
%%       {error, {Code::string, Msg::string(), ReqId::string()}}
%%
get_queue_attr(QueueUrl) ->
    try query_request(erlaws_cred:get(), QueueUrl, "GetQueueAttributes",  
		       [{"Attribute", "All"}]) of
	{ok, Body} -> 
	    {XmlDoc, _Rest} = xmerl_scan:string(Body),
	    AttributeNodes = xmerl_xpath:string("//AttributedValue", XmlDoc),
	    AttrList = [{Key, 
			 list_to_integer(Value)} || Node <- AttributeNodes,  
			begin
			    [#xmlText{value=Key}|_] = 
				xmerl_xpath:string("./Attribute/text()", Node),
			    [#xmlText{value=Value}|_] =
				xmerl_xpath:string("./Value/text()", Node),
			    true
			end],
	    {ok, AttrList}
    catch
	throw:{error, Descr} ->
	    {error, Descr}
    end.

%% This function allows you to alter the default VisibilityTimeout for
%% a given QueueUrl
%%
%% Spec: set_queue_attr(visibility_timeout, QueueUrl::string(), 
%%                      Timeout::integer()) ->
%%       {ok} |
%%       {error, {Code::string, Msg::string(), ReqId::string()}}
%%
set_queue_attr(visibility_timeout, QueueUrl, Timeout) 
  when is_integer(Timeout) ->
    try query_request(erlaws_cred:get(), QueueUrl, "SetQueueAttributes", 
		       [{"Attribute", "VisibilityTimeout"},
			{"Value", integer_to_list(Timeout)}]) of
	{ok, _Body} -> 
	    {ok}
    catch
	throw:{error, Descr} ->
	    {error, Descr}
    end.

%% Tries to deletes the queue identified by the given QueueUrl.
%% This functions fails if the given queue still contains undeleted
%% messages.
%%
%% Spec: delete_queue(QueueUrl::string()) ->
%%       {ok} |
%%       {error, {Code::string, Msg::string(), ReqId::string()}}
%%
delete_queue(Queue) ->
    delete_queue(Queue, false).

%% Tries to deletes the queue identified by the given QueueUrl.
%% This functions fails if the given queue still contains undeleted
%% messages and Force was set to "false".
%%
%% Spec: delete_queue(QueueUrl::string(), Force::boolean()) ->
%%       {ok} |
%%       {error, {Code::string, Msg::string(), ReqId::string()}}
%%
delete_queue(QueueUrl, Force) when is_boolean(Force) ->
    try query_request(erlaws_cred:get(), QueueUrl, "DeleteQueue", 
		       [{"ForceDeletion", atom_to_list(Force)}]) of
	{ok, _Body} -> 
	    {ok}
    catch
	throw:{error, Descr} ->
	    {error, Descr}
    end.

%% messages

%% Sends a message to the given QueueUrl. The message must not be greater
%% that 256 Kb or the call will fail.
%%
%% Spec: send_message(QueueUrl::string(), Message::string()) ->
%%       {ok, MessageId::string()} |
%%       {error, {Code::string, Msg::string(), ReqId::string()}}
%%
send_message(QueueUrl, Message) ->
    try rest_request(erlaws_cred:get(), put, QueueUrl ++ "/back", Message, 
		     []) of
	{ok, Body} -> 
	    {XmlDoc, _Rest} = xmerl_scan:string(Body),
	    [#xmlText{value=MessageId}|_] = 
		xmerl_xpath:string("//MessageId/text()", XmlDoc),
	    {ok, MessageId}
    catch
	throw:{error, Descr} ->
	    {error, Descr}
    end.

%% Peeks an messages identified by its MessageId from the given
%% Queue.
%%
%% Spec: peek_message(QueueUrl::string(), MessageId::string()) ->
%%       {ok, {MessageId::string(), MessageBody::string()}} |
%%       {error, {Code::string, Msg::string(), ReqId::string()}}
%%
peek_message(QueueUrl, MessageId) ->
    try query_request(erlaws_cred:get(), QueueUrl, "PeekMessage",  
		       [{"MessageId", MessageId}]) of
	{ok, Body} -> 
 	    {XmlDoc, _Rest} = xmerl_scan:string(Body),
 	    [#xmlText{value=MessageId}|_] = 
 		xmerl_xpath:string("//MessageId/text()", XmlDoc),
	    [#xmlText{value=MessageBody}|_] = 
		xmerl_xpath:string("//MessageBody/text()", XmlDoc),
 	    {ok, {MessageId, MessageBody}}
    catch
	throw:{error, Descr} ->
	    {error, Descr}
    end.

%% Tries to receive a single message from the given queue.
%%
%% Spec: receive_message(QueueUrl::string()) ->
%%       {ok, [{MessageId:string(), MessageBody::string()}]} |
%%       {ok, []}
%%       {error, {Code::string, Msg::string(), ReqId::string()}}
%%
receive_message(QueueUrl) ->
    receive_message(QueueUrl, 1).

%% Tries to receive up to NrOfMessages message from the given queue.
%%
%% Spec: receive_message(QueueUrl::string(), NrOfMessages::integer()) ->
%%       {ok, [{MessageId:string(), MessageBody::string()}]} |
%%       {ok, []}
%%       {error, {Code::string, Msg::string(), ReqId::string()}}
%%
receive_message(QueueUrl, NrOfMessages) when is_integer(NrOfMessages), 
					  NrOfMessages < 256 ->
    try rest_request(erlaws_cred:get(), get, QueueUrl ++ "/front", "", 
		     [{"NumberOfMessages", integer_to_list(NrOfMessages)}]) of
	{ok, Body} ->
	    {XmlDoc, _Rest} = xmerl_scan:string(Body),
	    MessageNodes = xmerl_xpath:string("//Message", XmlDoc),
	    {ok, [{MsgId, MsgBody} || Node <- MessageNodes,
				 begin
				     [#xmlText{value=MsgId}] =
					 xmerl_xpath:string("./MessageId/text()", Node),
				     [#xmlText{value=MsgBody}] =
					 xmerl_xpath:string("./MessageBody/text()", Node),
				     true
				 end]}
    catch
	throw:{error, Descr} ->
	    {error, Descr}
    end.

%% Deletes a message from a queue
%%
%% Spec: delete_message(QueueUrl::string(), MessageId::string()) ->
%%       {ok} |
%%       {error, {Code::string, Msg::string(), ReqId::string()}}
%%
delete_message(QueueUrl, MessageId) ->
    try query_request(erlaws_cred:get(), QueueUrl, "DeleteMessage",
		      [{"MessageId", MessageId}]) of
	{ok, _Body} ->
	    {ok}
    catch
	throw:{error, Descr} ->
	    {error, Descr}
    end.

%% internal methods

buildContentMD5Header(ContentMD5) ->
    case ContentMD5 of
	"" -> [];
	_ -> [{"Content-MD5", ContentMD5}]
    end.

buildContentHeaders( "", _ ) -> [];
buildContentHeaders( Contents, ContentType ) -> 
    [{"Content-Length", integer_to_list(length(Contents))},
     {"Content-Type", ContentType}].


stringToSign (Verb, ContentMD5, ContentType, Date, Path) ->
    Parts = [ Verb, ContentMD5, ContentType, Date, Path],
    erlaws_util:mkEnumeration( Parts, "\n").

sign (Key,Data) ->
    %%io:format("Sign:~n ~p~n", [Data]),
    binary_to_list( base64:encode( crypto:sha_mac(Key,Data) ) ).

rest_request({AccessKey, SecretAccessKey}, Method, Url, Message,
	     Parameters) ->

    Date = httpd_util:rfc1123_date(erlang:localtime()),
    MethodString = string:to_upper( atom_to_list(Method) ),

    ContentMD5 = case Message of
		     [] -> "";
		     _ -> binary_to_list(base64:encode(
					   erlang:md5(list_to_binary(Message))))
		 end,
    
    Headers = buildContentHeaders(Message, "text/plain" ) ++
	buildContentMD5Header(ContentMD5),
    
    Path = string:sub_string(Url, length(?AWS_SQS_URL)),

    Signature = sign(SecretAccessKey,
		     stringToSign( MethodString, ContentMD5, 
				   case ContentMD5 of 
				       "" -> "";
				       _ -> "text/plain"
				   end, 
				   Date, Path)),
    
    FinalHeaders = [ {"Authorization","AWS " ++ AccessKey ++ ":" ++ Signature },
		     {"Host", ?AWS_SQS_HOST },
		     {"Date", Date }
		     | Headers ],

    %%io:format("Headers:~n ~p~n", [FinalHeaders]),

    Result = mkReq(Method, Url, FinalHeaders, Parameters, "text/plain", 
		   Message),
    %%io:format("Result~n ~p~n", [Result]),
    
    case Result of
	{ok, _Status, Body} ->
	    {ok, Body};
	{error, {_Proto, 400, _Reason}, Body} ->
	    throw({error, mkErr(Body)});
	{error, {_Proto, Code, Reason}, _Body} ->
	    throw({error, {integer_to_list(Code), Reason, ""}})
    end.

query_request({AccessKey, SecretKey}, Url, Action, Parameters) ->
    Timestamp = lists:flatten(erlaws_util:get_timestamp()),
    Signature = sign(SecretKey, Action++Timestamp),
    FinalQueryParams = [{"AWSAccessKeyId", AccessKey},
			{"Action", Action}, 
			{"Version", "2007-05-01"},
			{"Timestamp", Timestamp},
			{"Signature", Signature}] ++ Parameters,
    Result = mkReq(get, Url, [], FinalQueryParams, "", ""),
    case Result of
	{ok, _Status, Body} ->
	    {ok, Body};
	{error, {_Proto, 400, _Reason}, Body} ->
	    throw({error, mkErr(Body)});
	{error, {_Proto, Code, Reason}, _Body} ->
	    throw({error, {integer_to_list(Code), Reason, ""}})
    end.

mkReq(Method, PreUrl, Headers, QueryParams, ContentType, ReqBody) ->
    %%io:format("QueryParams:~n ~p~nHeaders:~n ~p~nUrl:~n ~p~n", 
    %%      [QueryParams, Headers, PreUrl]),
    Url = PreUrl ++ erlaws_util:queryParams( QueryParams ),
    %%io:format("RequestUrl:~n ~p~n", [Url]),

    Request = case Method of
 		  get -> { Url, Headers };
 		  put -> { Url, Headers, ContentType, ReqBody }
 	      end,

    HttpOptions = [{autoredirect, true}],
    Options = [ {sync,true}, {headers_as_is,true}, {body_format, binary} ],
    {ok, {Status, _ReplyHeaders, Body}} = 
	http:request(Method, Request, HttpOptions, Options),
    %%io:format("Response:~n ~p~n", [binary_to_list(Body)]),
    case Status of 
	{_, 200, _} -> {ok, Status, binary_to_list(Body)};
	{_, _, _} -> {error, Status, binary_to_list(Body)}
    end.

mkErr(Xml) ->
    {XmlDoc, _Rest} = xmerl_scan:string( Xml ),
    [#xmlText{value=ErrorCode}|_] = xmerl_xpath:string("//Error/Code/text()", 
						       XmlDoc),
    ErrorMessage = 
	case xmerl_xpath:string("//Error/Message/text()", XmlDoc) of
	    [] -> "";
	    [EMsg|_] -> EMsg#xmlText.value
	end,
    [#xmlText{value=RequestId}|_] = xmerl_xpath:string("//RequestID/text()", 
						       XmlDoc),
    {ErrorCode, ErrorMessage, RequestId}.
