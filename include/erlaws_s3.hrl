%%
%% s3client record definitions
%%

-record( list_result, {isTruncated=false, keys=[], prefixes=[]}).
-record( object_info, {key, lastmodified, etag, size} ).
