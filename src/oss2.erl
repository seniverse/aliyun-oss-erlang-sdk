%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%   http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.

-module(oss2).

-export(
   [ set_config/2,
     head_object/2,
     get_object/2,
     get_object/3,
     put_object/3,
     copy_object/4,
     delete_object/2,
     initiate_multipart_upload/2,
     upload_part/5,
     copy_part/6,
     copy_part/7,
     complete_multipart_upload/4]).

set_config(Bucket, Config) ->
    ok = application:set_env(oss2, Bucket, Config).

get_config(Bucket) ->
    {ok, Config} = application:get_env(oss2, Bucket),
    Config.

head_object(Bucket, ObjectName) ->
    Request = {?FUNCTION_NAME, Bucket, ObjectName},
    case request(Bucket, <<"HEAD">>, ObjectName, []) of
        {ok, 200, _, ClientRef} ->
            handle_ok(ClientRef, Request);
        Other ->
            handle_error(Other, Request)
    end.

get_object(Bucket, ObjectName) ->
    Request = {?FUNCTION_NAME, Bucket, ObjectName},
    case request(Bucket, <<"GET">>, ObjectName, []) of
        {ok, 200, _, ClientRef} ->
            handle_body(ClientRef, Request);
        Other ->
            handle_error(Other, Request)
    end.

get_object(Bucket, ObjectName, {Start, Stop}) ->
    Request = {?FUNCTION_NAME, Bucket, ObjectName, {Start, Stop}},
    Headers = [{<<"Range">>, iolist_to_binary(io_lib:format("bytes=~B-~B", [Start, Stop]))}],
    case request(Bucket, <<"GET">>, ObjectName, Headers) of
        {ok, 206, _, ClientRef} ->
            handle_body(ClientRef, Request);
        Other ->
            handle_error(Other, Request)
    end.

put_object(Bucket, ObjectName, Content) ->
    Request = {?FUNCTION_NAME, Bucket, ObjectName},
    case request(Bucket, <<"PUT">>, ObjectName, [], Content) of
        {ok, 200, _, ClientRef} ->
            handle_ok(ClientRef, Request);
        Other ->
            handle_error(Other, Request)
    end.

copy_object(Bucket, ObjectName, SourceBucket, SourceObjectName) ->
    Request = {?FUNCTION_NAME, Bucket, ObjectName, SourceBucket, SourceObjectName},
    Headers = [{<<"x-oss-copy-source">>, [<<"/">>, SourceBucket, <<"/">>, SourceObjectName]}],
    case request(Bucket, <<"PUT">>, ObjectName, Headers) of
        {ok, 200, _, ClientRef} ->
            handle_ok(ClientRef, Request);
        Other ->
            handle_error(Other, Request)
    end.

delete_object(Bucket, ObjectName) ->
    Request = {?FUNCTION_NAME, Bucket, ObjectName},
    case request(Bucket, <<"DELETE">>, ObjectName, []) of
        {ok, 204, _, ClientRef} ->
            handle_ok(ClientRef, Request);
        Other ->
            handle_error(Other, Request)
    end.

initiate_multipart_upload(Bucket, ObjectName) ->
    Request = {?FUNCTION_NAME, Bucket, ObjectName},
    case request(Bucket, <<"POST">>, <<ObjectName/binary, "?uploads">>, []) of
        {ok, 200, _, ClientRef} ->
            case handle_body(ClientRef, Request) of
                {ok, Body} ->
                    {Doc, _} = xmerl_scan:string(binary_to_list(Body)),
                    [UploadId] = xmerl_xs:value_of(xmerl_xs:select("UploadId", Doc)),
                    {ok, UploadId};
                Other ->
                    Other
            end;
        Other ->
            handle_error(Other, Request)
    end.

upload_part(Bucket, ObjectName, UploadId, PartNumber, Content) ->
    Request = {?FUNCTION_NAME, Bucket, ObjectName, UploadId, PartNumber},
    Resource = [ObjectName, <<"?partNumber=">>, PartNumber, <<"&uploadId=">>, UploadId],
    case request(Bucket, <<"PUT">>, Resource, [], Content) of
        {ok, 200, _, ClientRef} ->
            handle_part(ClientRef, PartNumber, Request);
        Other ->
            handle_error(Other, Request)
    end.

copy_part(Bucket, ObjectName, UploadId, PartNumber, SourceBucket, SourceObjectName) ->
    Request = {?FUNCTION_NAME, Bucket, ObjectName, UploadId, PartNumber, SourceBucket, SourceObjectName},
    Resource = [ObjectName, <<"?partNumber=">>, PartNumber, <<"&uploadId=">>, UploadId],
    Headers = [{<<"x-oss-copy-source">>, [<<"/">>, SourceBucket, <<"/">>, SourceObjectName]}],
    case request(Bucket, <<"PUT">>, Resource, Headers) of
        {ok, 200, _, ClientRef} ->
            handle_part(ClientRef, PartNumber, Request);
        Other ->
            handle_error(Other, Request)
    end.


copy_part(Bucket, ObjectName, UploadId, PartNumber, SourceBucket, SourceObjectName, {Start, Stop}) ->
    Request = {?FUNCTION_NAME, Bucket, ObjectName, UploadId, PartNumber, SourceBucket, SourceObjectName},
    Resource = [ObjectName, <<"?partNumber=">>, PartNumber, <<"&uploadId=">>, UploadId],
    Headers = [{<<"x-oss-copy-source">>, [<<"/">>, SourceBucket, <<"/">>, SourceObjectName]},
               {<<"x-oss-copy-source-range">>, iolist_to_binary(io_lib:format("bytes=~B-~B", [Start, Stop]))}],
    case request(Bucket, <<"PUT">>, Resource, Headers) of
        {ok, 200, _, ClientRef} ->
            handle_part(ClientRef, PartNumber, Request);
        Other ->
            handle_error(Other, Request)
    end.

complete_multipart_upload(Bucket, ObjectName, UploadId, Parts) ->
    Request = {?FUNCTION_NAME, Bucket, ObjectName, UploadId},
    Content = xmerl:export_simple([{'CompleteMultipartUpload', Parts}], xmerl_xml),
    Resource = [ObjectName, <<"&uploadId=">>, UploadId],
    case request(Bucket, <<"POST">>, Resource, [], Content) of
        {ok, 200, _, ClientRef} ->
            handle_ok(ClientRef, Request);
        Other ->
            handle_error(Other, Request)
    end.


handle_ok(ClientRef, Request) ->
    case hackney:body(ClientRef) of
        {ok, _} ->
            ok;
        {error, Reason} ->
            {error, {oss2, Request, {body, Reason}}}
    end.


handle_body(ClientRef, Request) ->
    case hackney:body(ClientRef) of
        {ok, Body} ->
            {ok, Body};
        {error, Reason} ->
            {error, {oss2, Request, {body, Reason}}}
    end.

handle_part(ClientRef, PartNumber, Request) ->
    case hackney:body(ClientRef) of
        {ok, Body} ->
            {Doc, _} = xmerl_scan:string(binary_to_list(Body)),
            [ETag] = xmerl_xs:value_of(xmerl_xs:select("ETag", Doc)),
            {ok, {'Part', [{'PartNumber', [PartNumber]}, {'ETag', [ETag]}]}};
        {error, Reason} ->
            {error, {oss2, Request, {body, Reason}}}
    end.

handle_error({error, Reason}, Request) ->
    {error, {oss2, Request, {request, Reason}}};
handle_error({ok, StatusCode, _, ClientRef}, Request) ->
    hackney:body(ClientRef),
    {error, {oss2, Request, {http, StatusCode}}}.


canonical(Method, MD5, Type, Date, Headers, Bucket, Resource) ->
    iolist_to_binary(
          [Method, $\n,
           MD5, $\n,
           Type, $\n,
           Date, $\n,
           [ [K, $:, V, $\n]
             || {K, V} <- canonical_oss_headers(Headers) ],
           $/, Bucket, $/, Resource]).

canonical_oss_headers(Headers) ->
    Headers1 = [{string:lowercase(K), V} || {K, V} <- Headers],
    lists:sort([{K, V} || {<<"x-oss-", _/binary>> = K, V} <- Headers1]).

hackney_opts(Config) ->
    case Config of
        #{pool := Pool} ->
            [{pool, Pool}];
        _ ->
            []
    end.

request(BucketName, Method, Resource, Headers) ->
    Config = get_config(BucketName),
    #{access_key_id := Key,
      access_key_secret := Secret,
      bucket := Bucket,
      endpoint := Endpoint
     } = Config,
    hackney:request(
      Method,
      iolist_to_binary(io_lib:format("https://~s.~s/~s", [Bucket, Endpoint, Resource])),
      headers(Key, Secret, Method, Headers, Bucket, Resource),
      <<>>,
      hackney_opts(Config)).

request(BucketName, Method, Resource, Headers, Data) ->
    Config = get_config(BucketName),
    #{access_key_id := Key,
      access_key_secret := Secret,
      bucket := Bucket,
      endpoint := Endpoint
     } = Config,
    hackney:request(
      Method,
      iolist_to_binary(io_lib:format("https://~s.~s/~s", [Bucket, Endpoint, Resource])),
      headers(Key, Secret, Method, Headers, Bucket, Resource, Data),
      Data,
      hackney_opts(Config)).

sign(Secret, Method, MD5, Type, Date, Headers, Bucket, Resource) ->
    base64:encode(
      crypto:hmac(sha, Secret,
           canonical(Method, MD5, Type, Date, Headers, Bucket, Resource))).

headers(Key, Secret, Method, Headers, Bucket, Resource, Data) ->
    MD5 = base64:encode(crypto:hash(md5, Data)),
    [{<<"Content-MD5">>, MD5},
     headers(Key, Secret, Method, MD5, <<"application/octet-stream">>, Headers, Bucket, Resource)].

headers(Key, Secret, Method, Headers, Bucket, Resource) ->
    headers(Key, Secret, Method, <<>>, <<>>, Headers, Bucket, Resource).

headers(Key, Secret, Method, MD5, ContentType, Headers, Bucket, Resource) ->
    Date = httpd_util:rfc1123_date(),
    Signature = sign(Secret, Method, MD5, ContentType, Date, Headers, Bucket, Resource),
    [{<<"Date">>, iolist_to_binary(Date)},
     {<<"Authorization">>, iolist_to_binary([<<"OSS ">>, Key, $:, Signature])}
     |Headers].
