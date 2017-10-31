/**
 * Created by Heshan.i on 10/25/2017.
 */

var logger = require('dvp-common/LogHandler/CommonLogHandler.js').logger;
var resourceService = require('./services/ResourceService');
var redisHandler = require('./RedisHandler');
var tagHandler = require('./TagHandler');
var requestServerHandler = require('./RequestServerHandler');
var requestMetadataHandler = require('./RequestMetaDataHandler');
var requestQueueAndStatusHandler = require('./RequestQueueAndStatusHandler');
var routingService = require('./services/RoutingService');
var q = require('q');
var async = require('async');
var util = require('util');
var config = require('config');

var sortString = function (a, b) {
    a = a.toLowerCase();
    b = b.toLowerCase();
    if (a < b) return -1;
    if (a > b) return 1;
    return 0;
};

var preProcessRequestData = function (logKey, tenant, company, preRequestData) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - RequestHandler - PreProcessRequestData :: tenant: %d :: company: %d :: preRequestData: %j', logKey, tenant, company, preRequestData);

        if (preRequestData.RequestServerId && preRequestData.SessionId && preRequestData.Attributes && preRequestData.Attributes.length > 0) {

            var date = new Date();
            var requestObj = {
                Company: company,
                Tenant: tenant,
                SessionId: preRequestData.SessionId,
                ArriveTime: date.toISOString(),
                Priority: (preRequestData.Priority) ? preRequestData.Priority : '0',
                ResourceCount: (preRequestData.ResourceCount) ? preRequestData.ResourceCount : 1,
                OtherInfo: preRequestData.OtherInfo,
                LbIp: config.Host.LBIP,
                LbPort: config.Host.LBPort
            };

            requestServerHandler.GetRequestServer(logKey, tenant, company, preRequestData.RequestServerId).then(function (requestServerData) {

                requestObj.RequestServerId = requestServerData.ServerID;
                requestObj.ServerType = requestServerData.ServerType;
                requestObj.RequestType = requestServerData.RequestType;
                requestObj.RequestServerUrl = requestServerData.CallbackUrl;
                requestObj.CallbackOption = requestServerData.CallbackOption;
                requestObj.QPositionUrl = requestServerData.QueuePositionCallbackUrl;

                return requestMetadataHandler.GetRequestMetaData(logKey, tenant, company, requestServerData.ServerType, requestServerData.RequestType);

            }).then(function (requestMetadata) {

                if (requestMetadata && requestMetadata.AttributeMeta && requestMetadata.AttributeMeta.length > 0) {

                    if (requestMetadata.ServingAlgo && requestMetadata.HandlingAlgo && requestMetadata.SelectionAlgo && requestMetadata.ReqHandlingAlgo) {

                        var attributeDataList = [];
                        var requestAttributes = [];
                        var requestAttributeNames = [];

                        preRequestData.Attributes.forEach(function (requestAttributeId) {

                            requestMetadata.AttributeMeta.forEach(function (attributeMetadata) {

                                if (attributeMetadata && attributeMetadata.HandlingType === requestObj.RequestType && attributeMetadata.AttributeCode.indexOf(requestAttributeId) > -1) {

                                    var processedAttributeData = attributeDataList.filter(function (attributeData) {
                                        return attributeData.AttributeGroupName === attributeMetadata.AttributeGroupName && attributeData.HandlingType === attributeMetadata.HandlingType;
                                    });
                                    var attributeDetail = attributeMetadata.AttributeDetails.filter(function (attributeDetail) {
                                        if (attributeDetail.Id === requestAttributeId)
                                            return attributeDetail;
                                    });

                                    if (processedAttributeData && processedAttributeData.length > 0) {
                                        processedAttributeData[0].AttributeCode.push(requestAttributeId);
                                        requestAttributes.push(requestAttributeId);
                                        if (attributeDetail && attributeDetail.length > 0) {
                                            processedAttributeData[0].AttributeNames.push(attributeDetail[0].Name);
                                            requestAttributeNames.push(attributeDetail[0].Name);
                                        }
                                    } else {
                                        attributeDataList.push(
                                            {
                                                AttributeGroupName: attributeMetadata.AttributeGroupName,
                                                HandlingType: attributeMetadata.HandlingType,
                                                AttributeCode: [
                                                    requestAttributeId
                                                ],
                                                WeightPercentage: attributeMetadata.WeightPercentage,
                                                AttributeNames: (attributeDetail && attributeDetail.length > 0) ? [attributeDetail[0].Name] : []
                                            }
                                        );

                                        requestAttributes.push(requestAttributeId);
                                        if (attributeDetail && attributeDetail.length > 0)
                                            requestAttributeNames.push(attributeDetail[0].Name);
                                    }
                                }

                            });

                        });

                        if (attributeDataList.length > 0) {

                            requestObj.ServingAlgo = requestMetadata.ServingAlgo;
                            requestObj.HandlingAlgo = requestMetadata.HandlingAlgo;
                            requestObj.SelectionAlgo = requestMetadata.SelectionAlgo;
                            requestObj.ReqHandlingAlgo = requestMetadata.ReqHandlingAlgo;
                            requestObj.ReqSelectionAlgo = (requestMetadata.ReqSelectionAlgo) ? requestMetadata.ReqSelectionAlgo : 'LONGESTWAITING';
                            requestObj.AttributeInfo = attributeDataList;

                            var sortedRequestAttributes = requestAttributes.sort(sortString);
                            var attributeDataString = util.format('attribute_%s', sortedRequestAttributes.join(":attribute_"));
                            var queueId = util.format('Queue:%d:%d:%s:%s:%s:%s', requestObj.Company, requestObj.Tenant, requestObj.ServerType, requestObj.RequestType, attributeDataString, requestObj.Priority);
                            var queueSettingId = util.format('Queue:%d:%d:%s:%s:%s', requestObj.Company, requestObj.Tenant, requestObj.ServerType, requestObj.RequestType, attributeDataString);

                            requestObj.QueueId = queueId;

                            resourceService.GetQueueSetting(logKey, tenant, company, queueSettingId).then(function (queueSettingData) {

                                if (queueSettingData && queueSettingData.IsSuccess && queueSettingData.Result) {

                                    var queueSetting = queueSettingData.Result;
                                    requestObj.QPositionEnable = queueSetting.PublishPosition ? queueSetting.PublishPosition : false;
                                    requestObj.QueueName = queueSetting.QueueName ? queueSetting.QueueName : util.format('%s', requestAttributeNames.join("-"));

                                } else {

                                    requestObj.QPositionEnable = false;
                                    requestObj.QueueName = util.format('%s', requestAttributeNames.join("-"));

                                    resourceService.AddQueueSetting(logKey, tenant, company, requestObj.QueueName, sortedRequestAttributes, requestObj.ServerType, requestObj.RequestType);

                                }

                                logger.info('LogKey: %s - RequestHandler - PreProcessRequestData :: success', logKey);
                                deferred.resolve({Request: requestObj, Attributes: sortedRequestAttributes});

                            }).catch(function (ex) {

                                logger.error('LogKey: %s - RequestHandler - PreProcessRequestData - GetQueueSetting failed :: %s', logKey, ex);

                                requestObj.QPositionEnable = false;
                                requestObj.QueueName = util.format('%s', requestAttributeNames.join("-"));

                                deferred.resolve({Request: requestObj, Attributes: sortedRequestAttributes});

                            });

                        } else {

                            logger.error('LogKey: %s - RequestHandler - PreProcessRequestData failed :: Invalid request attributes', logKey);
                            deferred.reject('Invalid request attributes');

                        }

                    } else {

                        logger.error('LogKey: %s - RequestHandler - PreProcessRequestData failed :: Invalid request metadata', logKey);
                        deferred.reject('Invalid request metadata');
                    }

                } else {

                    logger.error('LogKey: %s - RequestHandler - PreProcessRequestData failed :: No registered attribute group found in request metadata', logKey);
                    deferred.reject('No registered attribute group found in request metadata');
                }

            }).catch(function (ex) {

                logger.error('LogKey: %s - RequestHandler - PreProcessRequestData - GetRequestServer failed :: %s', logKey, ex);
                deferred.reject(ex);
            });

        } else {

            logger.error('LogKey: %s - RequestHandler - PreProcessRequestData failed :: Insufficient data to process', logKey);
            deferred.reject('Insufficient data to process');
        }

    } catch (ex) {

        logger.error('LogKey: %s - RequestHandler - PreProcessRequestData failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};


var addRequest = function (logKey, tenant, company, preRequestData) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - RequestHandler - AddRequest :: tenant: %d :: company: %d :: preRequestData: %j', logKey, tenant, company, preRequestData);

        preProcessRequestData(logKey, tenant, company, preRequestData).then(function (preProcessResponse) {

            var requestObj = preProcessResponse.Request;
            var requestKey = util.format('Request:%d:%d:%s', requestObj.Tenant, requestObj.Company, requestObj.SessionId);
            var requestTags = [
                "company_" + requestObj.Company,
                "tenant_" + requestObj.Tenant,
                "sessionId_" + requestObj.SessionId,
                "reqServerId_" + requestObj.RequestServerId,
                "priority_" + requestObj.Priority,
                "objType_Request"
            ];

            preProcessResponse.Attributes.forEach(function (attribute) {
                requestTags.push(util.format('%s:attribute_%s', requestObj.RequestType, attribute));
            });

            redisHandler.R_Set(logKey, requestKey, JSON.stringify(requestObj)).then(function (requestResult) {

                logger.info('LogKey: %s - RequestHandler - AddRequest - R_Set request success :: %s', logKey, requestResult);
                return tagHandler.SetTags(logKey, 'Tag:Request', requestTags, requestKey);

            }).then(function (requestTagResult) {

                logger.info('LogKey: %s - RequestHandler - AddRequest - SetTags success :: %s', logKey, requestTagResult);

                switch (requestObj.ReqHandlingAlgo.toLowerCase()) {
                    case 'queue':

                        requestQueueAndStatusHandler.AddRequestToQueue(logKey, requestObj.Tenant, requestObj.Company, requestObj.RequestType, requestObj.QueueId, requestObj.SessionId).then(function (queuePosition) {

                            logger.error('LogKey: %s - RequestHandler - AddRequest - AddRequestToQueue success :: %s', logKey, queuePosition);
                            deferred.resolve({
                                Position: queuePosition,
                                QueueName: requestObj.QueueName,
                                Message: "Request added to queue. sessionId :: " + requestObj.SessionId
                            });

                        }).catch(function (ex) {

                            logger.error('LogKey: %s - RequestHandler - AddRequest - AddRequestToQueue failed :: %s', logKey, ex);
                            removeRequest(logKey, tenant, company, requestObj.SessionId, 'failed');
                            deferred.reject('Add Request to Queue Failed. sessionId :: ' + requestObj.SessionId);
                        });
                        break;

                    case 'direct':

                        var jsonOtherInfo = JSON.stringify(requestObj.OtherInfo);
                        routingService.PickResource(logKey, tenant, company, requestObj.ResourceCount, requestObj.SessionId, requestObj.ServerType, requestObj.RequestType, requestObj.SelectionAlgo, requestObj.HandlingAlgo, jsonOtherInfo).then(function (routingResponse) {

                            if(requestObj.ServingAlgo.toLowerCase() === 'callback') {

                                deferred.resolve('ARDS accept the request');

                            }else{

                                return continueRequest(logKey, requestObj.SessionId, routingResponse);
                            }

                        }).catch(function (ex) {

                            logger.error('LogKey: %s - RequestHandler - AddRequest - PickResource failed :: %s', logKey, ex);
                            removeRequest(logKey, tenant, company, requestObj.SessionId, 'failed');
                            deferred.reject('Pick Resource Failed. sessionId :: ' + requestObj.SessionId);
                        });
                        break;
                    default :

                        logger.error('LogKey: %s - RequestHandler - AddRequest - No request handling algorithm found', logKey);
                        removeRequest(logKey, tenant, company, requestObj.SessionId, 'failed');
                        deferred.reject('No request handling algorithm found');
                        break;

                }
            }).catch(function (ex) {

                logger.error('LogKey: %s - RequestHandler - AddRequest - set request data failed :: %s', logKey, ex);
                removeRequest(logKey, tenant, company, requestObj.SessionId, 'failed');
                deferred.reject('Add request failed :: Set request data');
            });

        }).catch(function (ex) {

            logger.error('LogKey: %s - RequestHandler - AddRequest - preProcessRequestData failed :: %s', logKey, ex);
            deferred.reject(ex);
        })

    } catch (ex) {

        logger.error('LogKey: %s - RequestHandler - AddRequest failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var removeRequest = function (logKey, tenant, company, sessionId, reason) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - ResourceHandler - RemoveRequest :: tenant: %d :: company: %d :: sessionId: %s :: reason: %s', logKey, tenant, company, sessionId, reason);

        var requestKey = util.format('Request:%d:%d:%s', tenant, company, sessionId);
        redisHandler.R_Get(logKey, requestKey).then(function (requestData) {

            if (requestData) {

                var requestObj = JSON.parse(requestData);
                var postAsyncTasks = [
                    function (callback) {
                        var pubRequestRemoved = util.format("EVENT:%s:%s:%s:%s:%s:%s:%s:%s:YYYY", tenant, company, "ARDS", "REQUEST", "REMOVED", reason, "", requestObj.SessionId);
                        redisHandler.R_Publish(logKey, 'events', pubRequestRemoved).then(function (result) {
                            callback(null, result);
                        }).catch(function (ex) {
                            callback(ex, null);
                        });
                    }
                ];

                if (requestObj.ReqHandlingAlgo === "QUEUE") {

                    postAsyncTasks.push(
                        function (callback) {
                            requestQueueAndStatusHandler.RemoveRequestFromQueue(logKey, tenant, company, requestObj.RequestType, requestObj.QueueId, requestObj.SessionId).then(function (result) {
                                if (requestObj.QPositionEnable) {
                                    requestServerHandler.SendPositionCallback(logKey, tenant, company, requestObj.RequestType, requestObj.QueueId, requestObj.QPositionUrl, requestObj.CallbackOption);
                                }
                                callback(null, result);
                            }).catch(function (ex) {
                                callback(ex, null);
                            });
                        }
                    );

                    if (reason == "NONE") {

                        var pubQueueId = requestObj.QueueId.replace(/:/g, "-");
                        var pubQueueAnswered = util.format("EVENT:%s:%s:%s:%s:%s:%s:%s:%s:YYYY", tenant, company, "ARDS", "QUEUE", "ANSWERED", pubQueueId, "", requestObj.SessionId);
                        postAsyncTasks.push(
                            function (callback) {
                                redisHandler.R_Publish(logKey, 'events', pubQueueAnswered).then(function (result) {
                                    callback(null, result);
                                }).catch(function (ex) {
                                    callback(ex, null);
                                });
                            }
                        );
                    }
                }

                requestQueueAndStatusHandler.RemoveRequestStatus(logKey, tenant, company, requestObj.SessionId).then(function (requestStatusResult) {

                    logger.info('LogKey: %s - RequestHandler - RemoveRequest - RemoveRequestStatus process :: %s', logKey, requestKey, requestStatusResult);
                    return tagHandler.RemoveTags(logKey, requestKey);

                }).then(function (removeTagsResult) {

                    logger.info('LogKey: %s - RequestHandler - RemoveRequest - Remove %s tag process :: %s', logKey, requestKey, removeTagsResult);
                    return redisHandler.R_Del(logKey, requestKey);

                }).then(function (result) {

                    async.parallel(async.reflectAll(postAsyncTasks), function () {

                        logger.info('LogKey: %s - RequestHandler - RemoveRequest - Remove %s request process finished:: %s', logKey, requestKey, result);
                        deferred.resolve('Remove request process finished');

                    });

                }).catch(function (ex) {

                    logger.info('LogKey: %s - RequestHandler - RemoveRequest - Remove %s request process failed :: %s', logKey, requestKey, ex);
                    deferred.reject('Remove request process failed')

                });

            } else {

                logger.error('LogKey: %s - RequestHandler - RemoveRequest - No request found', logKey);
                deferred.reject('No request found');
            }

        }).catch(function (ex) {

            logger.error('LogKey: %s - RequestHandler - RemoveRequest - R_Get failed :: %s', logKey, ex);
            deferred.reject(ex);
        })

    } catch (ex) {

        logger.error('LogKey: %s - RequestHandler - RemoveRequest failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var rejectRequest = function (logKey, tenant, company, sessionId, reason) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - ResourceHandler - RejectRequest :: tenant: %d :: company: %d :: sessionId: %s :: reason: %s', logKey, tenant, company, sessionId, reason);

        var requestKey = util.format('Request:%d:%d:%s', tenant, company, sessionId);
        redisHandler.R_Get(logKey, requestKey).then(function (requestData) {

            if (requestData) {

                var requestObj = JSON.parse(requestData);
                if (reason == "NoSession" || reason == "ClientRejected") {

                    var pubQueueId = requestObj.QueueId.replace(/:/g, "-");
                    var pubMessage = util.format("EVENT:%s:%s:%s:%s:%s:%s:%s:%s:YYYY", tenant, company, "ARDS", "QUEUE", "DROPPED", pubQueueId, "", requestObj.SessionId);
                    redisHandler.R_Publish(logKey, "events", pubMessage);

                    return removeRequest(logKey, tenant, company, requestObj.SessionId, reason);

                } else {

                    return requestQueueAndStatusHandler.AddRequestToRejectQueue(logKey, tenant, company, requestObj.RequestType, requestObj.QueueId, requestObj.SessionId);

                }

            } else {

                logger.error('LogKey: %s - RequestHandler - RejectRequest - No request found', logKey);
                deferred.reject('No request found');
            }

        }).catch(function (ex) {

            logger.error('LogKey: %s - RequestHandler - RejectRequest - R_Get failed :: %s', logKey, ex);
            deferred.reject(ex);
        })

    } catch (ex) {

        logger.error('LogKey: %s - RequestHandler - RejectRequest failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var continueRequest = function (logKey, tenant, company, sessionId, routingResponse) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - RequestHandler - ContinueRequest :: tenant: %d :: company: %d :: sessionId: %s :: routingResponse: %j', logKey, tenant, company, sessionId, routingResponse);

        if (routingResponse && routingResponse != "" && routingResponse != "No matching resources at the moment") {

            var requestKey = util.format('Request:%d:%d:%s', tenant, company, sessionId);
            redisHandler.R_Get(logKey, requestKey).then(function (requestData) {

                if (requestData) {

                    var requestObj = JSON.parse(requestData);
                    var routingResponseObj = JSON.parse(routingResponse);

                    var routeObj = {
                        Company: requestObj.Company.toString(),
                        Tenant: requestObj.Tenant.toString(),
                        ServerType: requestObj.ServerType,
                        RequestType: requestObj.RequestType,
                        SessionID: requestObj.SessionId,
                        Skills: requestObj.QueueName,
                        OtherInfo: requestObj.OtherInfo
                    };

                    if (Array.isArray(routingResponseObj)) {
                        var resInfoData = [];
                        routingResponseObj.forEach(function (resData) {
                            resInfoData.push(JSON.parse(resData));
                        });

                        routeObj.ResourceInfo = resInfoData;

                    } else {

                        routeObj.ResourceInfo = routingResponseObj;

                    }

                    switch (requestObj.ServingAlgo.toLowerCase()) {
                        case "callback":

                            var asyncTasks = [
                                function (callback) {
                                    requestQueueAndStatusHandler.SetRequestStatus(logKey, requestObj.Tenant, requestObj.Company, requestObj.SessionId, 'TRYING').then(function (result) {

                                        logger.info('LogKey: %s - RequestHandler - ContinueRequest - SetRequestStatus success :: %s', logKey, result);
                                        callback(null, result);

                                    }).catch(function (ex) {

                                        logger.error('LogKey: %s - RequestHandler - ContinueRequest - SetRequestStatus failed :: %s', logKey, ex);
                                        callback(ex, null);
                                    });
                                }
                            ];

                            if (requestObj.ReqHandlingAlgo == "QUEUE") {

                                asyncTasks.push(
                                    function (callback) {
                                        var processingHashKey = util.format('ProcessingHash:%d:%d:%s', requestObj.Tenant, requestObj.Company, requestObj.RequestType);
                                        requestQueueAndStatusHandler.SetNextProcessingItem(logKey, requestObj.QueueId, processingHashKey, requestObj.SessionId).then(function (result) {

                                            if (requestObj.QPositionEnable) {
                                                requestServerHandler.SendPositionCallback(logKey, requestObj.Tenant, requestObj.Company, requestObj.RequestType, requestObj.QueueId, requestObj.QPositionUrl, requestObj.CallbackOption);
                                            }
                                            logger.info('LogKey: %s - RequestHandler - ContinueRequest - SetNextProcessingItem success :: %s', logKey, result);
                                            callback(null, result);

                                        }).catch(function (ex) {

                                            logger.error('LogKey: %s - RequestHandler - ContinueRequest - SetNextProcessingItem failed :: %s', logKey, ex);
                                            callback(ex, null);
                                        });
                                    }
                                );

                            }

                            async.parallel(async.reflectAll(asyncTasks), function () {

                                requestServerHandler.SendRoutingCallback(logKey, requestObj.RequestServerUrl, requestObj.CallbackOption, routeObj).then(function (result) {

                                    if(result && result === 're-addRequired'){

                                        logger.error('LogKey: %s - RequestHandler - ContinueRequest - SendRoutingCallback failed, continue reject request', logKey);
                                        rejectRequest(logKey, requestObj.Tenant, requestObj.Company, requestObj.SessionId, 'Send callback failed');

                                    }else{
                                        logger.info('LogKey: %s - RequestHandler - ContinueRequest - SendRoutingCallback success', logKey);
                                    }

                                }).catch(function (ex) {

                                    logger.error('LogKey: %s - RequestHandler - ContinueRequest - SendRoutingCallback failed, continue reject request :: %s', logKey, ex);
                                    rejectRequest(logKey, requestObj.Tenant, requestObj.Company, requestObj.SessionId, 'Send callback failed');
                                });

                                deferred.resolve('ARDS continue process finished');

                            });

                            break;

                        default :

                            logger.info('LogKey: %s - RequestHandler - ContinueRequest - pick resource completed :: %j', logKey, routeObj);
                            deferred.resolve(routeObj);
                            break;
                    }
                } else {

                    logger.error('LogKey: %s - RequestHandler - ContinueRequest - No request found :: %s', logKey, requestKey);
                    deferred.reject('No request found');
                }

            }).catch(function (ex) {

                logger.error('LogKey: %s - RequestHandler - ContinueRequest - R_Get failed :: %s', logKey, ex);
                deferred.reject(ex);
            });

        } else {

            logger.info('LogKey: %s - RequestHandler - ContinueRequest - No matching resources at the moment', logKey);
            deferred.reject('No matching resources at the moment');
        }


    } catch (ex) {

        logger.error('LogKey: %s - RequestHandler - ContinueRequest failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

module.exports.AddRequest = addRequest;
module.exports.RemoveRequest = removeRequest;
module.exports.RejectRequest = rejectRequest;
module.exports.ContinueRequest = continueRequest;