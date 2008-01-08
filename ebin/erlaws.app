{application, erlaws, 
 [{description, "Amazon WebServices Client"}, 
  {vsn, "0.1.2"}, 
  {modules, [erlaws_cred, erlaws_sdb, erlaws_s3, 
	     erlaws_sqs, erlaws_util]}, 
  {registered, [erlaws_cred]}, 
  {applications, [kernel, stdlib, sasl, crypto, inets]}, 
  {mod, {erlaws,[]}} 
 ]}. 

