/**
 * Created by Heshan.i on 10/31/2017.
 */

var logger = require('dvp-common/LogHandler/CommonLogHandler.js').logger;
var messageFormatter = require('dvp-common/CommonMessageGenerator/ClientMessageJsonFormatter');
var restify = require('restify');
var jwt = require('restify-jwt');
var secret = require('dvp-common/Authentication/Secret');
var authorization = require('dvp-common/Authentication/Authorization');
var uuid = require('uuid/v4');
var config = require('config');

var requestServerHandler = require('./RequestServerHandler');
var requestMetadataHandler = require('./RequestMetaDataHandler');
var requestHandler = require('./RequestHandler');
var resourceHandler = require('./ResourceHandler');
var resourceStatusMapper = require('./ResourceStatusMapper');

var server = restify.createServer({
    name: 'ArdsLiteServiceImproved',
    version: '1.0.0'
});

restify.CORS.ALLOW_HEADERS.push('authorization');
server.use(restify.CORS());
server.use(restify.fullResponse());
server.use(restify.acceptParser(server.acceptable));
server.use(restify.queryParser());
server.use(restify.bodyParser());
server.use(jwt({secret: secret.Secret}));



//---------------------------------Request Server----------------------------------------

server.post('/DVP/API/:version/ARDS/requestserver',authorization({resource:"requestserver", action:"write"}), function (req, res, next) {

    var jsonString;
    var logKey = uuid();
    try {
        logger.info('+++++++++++++++LogKey: %s - Start post request - AddRequestServer - req.body :: %j', logKey, req.body);

        var tenant = parseInt(req.user.tenant);
        var company = parseInt(req.user.company);
        requestServerHandler.AddRequestServer(logKey, tenant, company, req.body).then(function (result) {

            logger.info('+++++++++++++++LogKey: %s - End post request - AddRequestServer success :: %s', logKey, result);
            jsonString = messageFormatter.FormatMessage(undefined, "Add request server success", true, result);
            res.end(jsonString);

        }).catch(function (ex) {

            logger.error('+++++++++++++++LogKey: %s - End post request - AddRequestServer failed :: %s', logKey, ex);
            if(ex.message) {
                jsonString = messageFormatter.FormatMessage(ex, "Add request server failed", false, undefined);
            }else{
                jsonString = messageFormatter.FormatMessage(undefined, ex, false, undefined);
            }
            res.end(jsonString);
        });

    } catch (ex) {

        logger.error('+++++++++++++++LogKey: %s - End post request - AddRequestServer failed :: %s', logKey, ex);
        jsonString = messageFormatter.FormatMessage(ex, "Add request server failed", false, undefined);
        res.end(jsonString);
    }

    return next();
});

server.put('/DVP/API/:version/ARDS/requestserver',authorization({resource:"requestserver", action:"write"}), function (req, res, next) {

    var jsonString;
    var logKey = uuid();
    try {
        logger.info('+++++++++++++++LogKey: %s - Start put request - SetRequestServer - req.body :: %j', logKey, req.body);

        var tenant = parseInt(req.user.tenant);
        var company = parseInt(req.user.company);
        requestServerHandler.SetRequestServer(logKey, tenant, company, req.body).then(function (result) {

            logger.info('+++++++++++++++LogKey: %s - End put request - SetRequestServer success :: %s', logKey, result);
            jsonString = messageFormatter.FormatMessage(undefined, "Set request server success", true, result);
            res.end(jsonString);

        }).catch(function (ex) {

            logger.error('+++++++++++++++LogKey: %s - End put request - SetRequestServer failed :: %s', logKey, ex);
            if(ex.message) {
                jsonString = messageFormatter.FormatMessage(ex, "Set request server failed", false, undefined);
            }else{
                jsonString = messageFormatter.FormatMessage(undefined, ex, false, undefined);
            }
            res.end(jsonString);
        });

    } catch (ex) {

        logger.error('+++++++++++++++LogKey: %s - End put request - SetRequestServer failed :: %s', logKey, ex);
        jsonString = messageFormatter.FormatMessage(ex, "Set request server failed", false, undefined);
        res.end(jsonString);
    }

    return next();
});

server.get('/DVP/API/:version/ARDS/requestserver/:serverid',authorization({resource:"requestserver", action:"read"}), function (req, res, next) {

    var jsonString;
    var logKey = uuid();
    try {
        logger.info('+++++++++++++++LogKey: %s - Start get request - GetRequestServer - req.params :: %j', logKey, req.params);

        var tenant = parseInt(req.user.tenant);
        var company = parseInt(req.user.company);
        requestServerHandler.GetRequestServer(logKey, tenant, company, req.params.serverid).then(function (result) {

            logger.info('+++++++++++++++LogKey: %s - End get request - GetRequestServer success :: %s', logKey, result);
            jsonString = messageFormatter.FormatMessage(undefined, "Get request server success", true, result);
            res.end(jsonString);

        }).catch(function (ex) {

            logger.error('+++++++++++++++LogKey: %s - End get request - GetRequestServer failed :: %s', logKey, ex);
            if(ex.message) {
                jsonString = messageFormatter.FormatMessage(ex, "Get request server failed", false, undefined);
            }else{
                jsonString = messageFormatter.FormatMessage(undefined, ex, false, undefined);
            }
            res.end(jsonString);
        });

    } catch (ex) {

        logger.error('+++++++++++++++LogKey: %s - End get request - GetRequestServer failed :: %s', logKey, ex);
        jsonString = messageFormatter.FormatMessage(ex, "Get request server failed", false, undefined);
        res.end(jsonString);
    }

    return next();
});

server.get('/DVP/API/:version/ARDS/requestservers/:serverType/:requestType',authorization({resource:"requestserver", action:"read"}), function (req, res, next) {

    var jsonString;
    var logKey = uuid();
    try {
        logger.info('+++++++++++++++LogKey: %s - Start get request - GetRequestServerByType - req.params :: %j', logKey, req.params);

        var tenant = parseInt(req.user.tenant);
        var company = parseInt(req.user.company);
        requestServerHandler.GetRequestServerByType(logKey, tenant, company, req.params.serverType, req.params.requestType).then(function (result) {

            logger.info('+++++++++++++++LogKey: %s - End get request - GetRequestServerByType success :: %s', logKey, result);
            jsonString = messageFormatter.FormatMessage(undefined, "Get request servers success", true, result);
            res.end(jsonString);

        }).catch(function (ex) {

            logger.error('+++++++++++++++LogKey: %s - End get request - GetRequestServerByType failed :: %s', logKey, ex);
            if(ex.message) {
                jsonString = messageFormatter.FormatMessage(ex, "Get request servers failed", false, undefined);
            }else{
                jsonString = messageFormatter.FormatMessage(undefined, ex, false, undefined);
            }
            res.end(jsonString);
        });

    } catch (ex) {

        logger.error('+++++++++++++++LogKey: %s - End get request - GetRequestServerByType failed :: %s', logKey, ex);
        jsonString = messageFormatter.FormatMessage(ex, "Get request servers failed", false, undefined);
        res.end(jsonString);
    }

    return next();
});

server.del('/DVP/API/:version/ARDS/requestserver/:serverid',authorization({resource:"requestserver", action:"delete"}), function (req, res, next) {

    var jsonString;
    var logKey = uuid();
    try {
        logger.info('+++++++++++++++LogKey: %s - Start delete request - RemoveRequestServer - req.params :: %j', logKey, req.params);

        var tenant = parseInt(req.user.tenant);
        var company = parseInt(req.user.company);
        requestServerHandler.RemoveRequestServer(logKey, tenant, company, req.params.serverid).then(function (result) {

            logger.info('+++++++++++++++LogKey: %s - End delete request - RemoveRequestServer success :: %s', logKey, result);
            jsonString = messageFormatter.FormatMessage(undefined, "Remove request server success", true, result);
            res.end(jsonString);

        }).catch(function (ex) {

            logger.error('+++++++++++++++LogKey: %s - End delete request - RemoveRequestServer failed :: %s', logKey, ex);
            if(ex.message) {
                jsonString = messageFormatter.FormatMessage(ex, "Remove request server failed", false, undefined);
            }else{
                jsonString = messageFormatter.FormatMessage(undefined, ex, false, undefined);
            }
            res.end(jsonString);
        });

    } catch (ex) {

        logger.error('+++++++++++++++LogKey: %s - End delete request - RemoveRequestServer failed :: %s', logKey, ex);
        jsonString = messageFormatter.FormatMessage(ex, "Remove request server failed", false, undefined);
        res.end(jsonString);
    }

    return next();
});


//---------------------------------Request Metadata---------------------------------------

server.post('/DVP/API/:version/ARDS/requestmeta',authorization({resource:"requestmeta", action:"write"}), function (req, res, next) {

    var jsonString;
    var logKey = uuid();
    try {
        logger.info('+++++++++++++++LogKey: %s - Start post request - AddRequestMetaData - req.body :: %j', logKey, req.body);

        var tenant = parseInt(req.user.tenant);
        var company = parseInt(req.user.company);
        requestMetadataHandler.AddRequestMetaData(logKey, tenant, company, req.body).then(function (result) {

            logger.info('+++++++++++++++LogKey: %s - End post request - AddRequestMetaData success :: %s', logKey, result);
            jsonString = messageFormatter.FormatMessage(undefined, "Add request metadata success", true, result);
            res.end(jsonString);

        }).catch(function (ex) {

            logger.error('+++++++++++++++LogKey: %s - End post request - AddRequestMetaData failed :: %s', logKey, ex);
            if(ex.message) {
                jsonString = messageFormatter.FormatMessage(ex, "Add request metadata failed", false, undefined);
            }else{
                jsonString = messageFormatter.FormatMessage(undefined, ex, false, undefined);
            }
            res.end(jsonString);
        });

    } catch (ex) {

        logger.error('+++++++++++++++LogKey: %s - End post request - AddRequestMetaData failed :: %s', logKey, ex);
        jsonString = messageFormatter.FormatMessage(ex, "Add request metadata failed", false, undefined);
        res.end(jsonString);
    }

    return next();
});

server.put('/DVP/API/:version/ARDS/requestmeta',authorization({resource:"requestmeta", action:"write"}), function (req, res, next) {

    var jsonString;
    var logKey = uuid();
    try {
        logger.info('+++++++++++++++LogKey: %s - Start put request - AddRequestMetaData - req.body :: %j', logKey, req.body);

        var tenant = parseInt(req.user.tenant);
        var company = parseInt(req.user.company);
        requestMetadataHandler.AddRequestMetaData(logKey, tenant, company, req.body).then(function (result) {

            logger.info('+++++++++++++++LogKey: %s - End put request - AddRequestMetaData success :: %s', logKey, result);
            jsonString = messageFormatter.FormatMessage(undefined, "Set request metadata success", true, result);
            res.end(jsonString);

        }).catch(function (ex) {

            logger.error('+++++++++++++++LogKey: %s - End put request - AddRequestMetaData failed :: %s', logKey, ex);
            if(ex.message) {
                jsonString = messageFormatter.FormatMessage(ex, "Set request metadata failed", false, undefined);
            }else{
                jsonString = messageFormatter.FormatMessage(undefined, ex, false, undefined);
            }
            res.end(jsonString);
        });

    } catch (ex) {

        logger.error('+++++++++++++++LogKey: %s - End put request - AddRequestMetaData failed :: %s', logKey, ex);
        jsonString = messageFormatter.FormatMessage(ex, "Set request metadata failed", false, undefined);
        res.end(jsonString);
    }

    return next();
});

server.get('/DVP/API/:version/ARDS/requestmeta/:serverType/:requestType',authorization({resource:"requestmeta", action:"read"}), function (req, res, next) {

    var jsonString;
    var logKey = uuid();
    try {
        logger.info('+++++++++++++++LogKey: %s - Start get request - GetRequestMetaData - req.params :: %j', logKey, req.params);

        var tenant = parseInt(req.user.tenant);
        var company = parseInt(req.user.company);
        requestMetadataHandler.GetRequestMetaData(logKey, tenant, company, req.params.serverType, req.params.requestType).then(function (result) {

            logger.info('+++++++++++++++LogKey: %s - End get request - GetRequestMetaData success :: %s', logKey, result);
            jsonString = messageFormatter.FormatMessage(undefined, "Get request metadata success", true, result);
            res.end(jsonString);

        }).catch(function (ex) {

            logger.error('+++++++++++++++LogKey: %s - End get request - GetRequestMetaData failed :: %s', logKey, ex);
            if(ex.message) {
                jsonString = messageFormatter.FormatMessage(ex, "Get request metadata failed", false, undefined);
            }else{
                jsonString = messageFormatter.FormatMessage(undefined, ex, false, undefined);
            }
            res.end(jsonString);
        });

    } catch (ex) {

        logger.error('+++++++++++++++LogKey: %s - End get request - GetRequestMetaData failed :: %s', logKey, ex);
        jsonString = messageFormatter.FormatMessage(ex, "Get request metadata failed", false, undefined);
        res.end(jsonString);
    }

    return next();
});

server.get('/DVP/API/:version/ARDS/requestmeta',authorization({resource:"requestmeta", action:"read"}), function (req, res, next) {

    var jsonString;
    var logKey = uuid();
    try {
        logger.info('+++++++++++++++LogKey: %s - Start get request - GetAllRequestMetaData - req.params :: %j', logKey, req.params);

        var tenant = parseInt(req.user.tenant);
        var company = parseInt(req.user.company);
        requestMetadataHandler.GetAllRequestMetaData(logKey, tenant, company).then(function (result) {

            logger.info('+++++++++++++++LogKey: %s - End get request - GetAllRequestMetaData success :: %s', logKey, result);
            jsonString = messageFormatter.FormatMessage(undefined, "Get request metadata success", true, result);
            res.end(jsonString);

        }).catch(function (ex) {

            logger.error('+++++++++++++++LogKey: %s - End get request - GetAllRequestMetaData failed :: %s', logKey, ex);
            if(ex.message) {
                jsonString = messageFormatter.FormatMessage(ex, "Get request metadata failed", false, undefined);
            }else{
                jsonString = messageFormatter.FormatMessage(undefined, ex, false, undefined);
            }
            res.end(jsonString);
        });

    } catch (ex) {

        logger.error('+++++++++++++++LogKey: %s - End get request - GetAllRequestMetaData failed :: %s', logKey, ex);
        jsonString = messageFormatter.FormatMessage(ex, "Get request metadata failed", false, undefined);
        res.end(jsonString);
    }

    return next();
});

server.del('/DVP/API/:version/ARDS/requestmeta/:serverType/:requestType',authorization({resource:"requestmeta", action:"delete"}), function (req, res, next) {

    var jsonString;
    var logKey = uuid();
    try {
        logger.info('+++++++++++++++LogKey: %s - Start delete request - RemoveRequestMetaData - req.params :: %j', logKey, req.params);

        var tenant = parseInt(req.user.tenant);
        var company = parseInt(req.user.company);
        requestMetadataHandler.RemoveRequestMetaData(logKey, tenant, company, req.params.serverType, req.params.requestType).then(function (result) {

            logger.info('+++++++++++++++LogKey: %s - End delete request - RemoveRequestMetaData success :: %s', logKey, result);
            jsonString = messageFormatter.FormatMessage(undefined, "Remove request metadata success", true, result);
            res.end(jsonString);

        }).catch(function (ex) {

            logger.error('+++++++++++++++LogKey: %s - End delete request - RemoveRequestMetaData failed :: %s', logKey, ex);
            if(ex.message) {
                jsonString = messageFormatter.FormatMessage(ex, "Remove request metadata failed", false, undefined);
            }else{
                jsonString = messageFormatter.FormatMessage(undefined, ex, false, undefined);
            }
            res.end(jsonString);
        });

    } catch (ex) {

        logger.error('+++++++++++++++LogKey: %s - End delete request - RemoveRequestMetaData failed :: %s', logKey, ex);
        jsonString = messageFormatter.FormatMessage(ex, "Remove request metadata failed", false, undefined);
        res.end(jsonString);
    }

    return next();
});


//---------------------------------Request-----------------------------------------------

server.post('/DVP/API/:version/ARDS/request',authorization({resource:"ardsrequest", action:"write"}), function (req, res, next) {

    var jsonString;
    var logKey = uuid();
    try {
        logger.info('+++++++++++++++LogKey: %s - Start post request - AddRequest - req.body :: %j', logKey, req.body);

        var tenant = parseInt(req.user.tenant);
        var company = parseInt(req.user.company);
        requestHandler.AddRequest(logKey, tenant, company, req.body).then(function (result) {

            logger.info('+++++++++++++++LogKey: %s - End post request - AddRequest success :: %s', logKey, result);
            jsonString = messageFormatter.FormatMessage(undefined, "Add request success", true, result);
            res.end(jsonString);

        }).catch(function (ex) {

            logger.error('+++++++++++++++LogKey: %s - End post request - AddRequest failed :: %s', logKey, ex);
            if(ex.message) {
                jsonString = messageFormatter.FormatMessage(ex, "Add request failed", false, undefined);
            }else{
                jsonString = messageFormatter.FormatMessage(undefined, ex, false, undefined);
            }
            res.end(jsonString);
        });

    } catch (ex) {

        logger.error('+++++++++++++++LogKey: %s - End post request - AddRequest failed :: %s', logKey, ex);
        jsonString = messageFormatter.FormatMessage(ex, "Add request failed", false, undefined);
        res.end(jsonString);
    }

    return next();
});

server.post('/DVP/API/:version/ARDS/continueprocess',authorization({resource:"ardsrequest", action:"write"}), function (req, res, next) {

    var jsonString;
    var logKey = uuid();
    try {
        logger.info('+++++++++++++++LogKey: %s - Start post request - ContinueRequest - req.body :: %j', logKey, req.body);

        var tenant = parseInt(req.user.tenant);
        var company = parseInt(req.user.company);
        requestHandler.ContinueRequest(logKey, tenant, company, req.body.SessionId, req.body.HandlingResource).then(function (result) {

            logger.info('+++++++++++++++LogKey: %s - End post request - ContinueRequest success :: %s', logKey, result);
            jsonString = messageFormatter.FormatMessage(undefined, "Continue request success", true, result);
            res.end(jsonString);

        }).catch(function (ex) {

            logger.error('+++++++++++++++LogKey: %s - End post request - ContinueRequest failed :: %s', logKey, ex);
            if(ex.message) {
                jsonString = messageFormatter.FormatMessage(ex, "Continue request failed", false, undefined);
            }else{
                jsonString = messageFormatter.FormatMessage(undefined, ex, false, undefined);
            }
            res.end(jsonString);
        });

    } catch (ex) {

        logger.error('+++++++++++++++LogKey: %s - End post request - ContinueRequest failed :: %s', logKey, ex);
        jsonString = messageFormatter.FormatMessage(ex, "Continue request failed", false, undefined);
        res.end(jsonString);
    }

    return next();
});

server.del('/DVP/API/:version/ARDS/request/:sessionid/:reason',authorization({resource:"ardsrequest", action:"delete"}), function (req, res, next) {

    var jsonString;
    var logKey = uuid();
    try {
        logger.info('+++++++++++++++LogKey: %s - Start delete request - RemoveRequest - req.params :: %j', logKey, req.params);

        var tenant = parseInt(req.user.tenant);
        var company = parseInt(req.user.company);
        requestHandler.RemoveRequest(logKey, tenant, company, req.params.sessionid, req.params.reason).then(function (result) {

            logger.info('+++++++++++++++LogKey: %s - End delete request - RemoveRequest success :: %s', logKey, result);
            jsonString = messageFormatter.FormatMessage(undefined, "Remove request success", true, result);
            res.end(jsonString);

        }).catch(function (ex) {

            logger.error('+++++++++++++++LogKey: %s - End delete request - RemoveRequest failed :: %s', logKey, ex);
            if(ex.message) {
                jsonString = messageFormatter.FormatMessage(ex, "Remove request failed", false, undefined);
            }else{
                jsonString = messageFormatter.FormatMessage(undefined, ex, false, undefined);
            }
            res.end(jsonString);
        });

    } catch (ex) {

        logger.error('+++++++++++++++LogKey: %s - End delete request - RemoveRequest failed :: %s', logKey, ex);
        jsonString = messageFormatter.FormatMessage(ex, "Remove request failed", false, undefined);
        res.end(jsonString);
    }

    return next();
});

server.del('/DVP/API/:version/ARDS/request/:sessionid/reject/:reason',authorization({resource:"ardsrequest", action:"delete"}), function (req, res, next) {

    var jsonString;
    var logKey = uuid();
    try {
        logger.info('+++++++++++++++LogKey: %s - Start delete request - RejectRequest - req.params :: %j', logKey, req.params);

        var tenant = parseInt(req.user.tenant);
        var company = parseInt(req.user.company);
        requestHandler.RejectRequest(logKey, tenant, company, req.params.sessionid, req.params.reason).then(function (result) {

            logger.info('+++++++++++++++LogKey: %s - End delete request - RejectRequest success :: %s', logKey, result);
            jsonString = messageFormatter.FormatMessage(undefined, "Reject request success", true, result);
            res.end(jsonString);

        }).catch(function (ex) {

            logger.error('+++++++++++++++LogKey: %s - End delete request - RejectRequest failed :: %s', logKey, ex);
            if(ex.message) {
                jsonString = messageFormatter.FormatMessage(ex, "Reject request failed", false, undefined);
            }else{
                jsonString = messageFormatter.FormatMessage(undefined, ex, false, undefined);
            }
            res.end(jsonString);
        });

    } catch (ex) {

        logger.error('+++++++++++++++LogKey: %s - End delete request - RejectRequest failed :: %s', logKey, ex);
        jsonString = messageFormatter.FormatMessage(ex, "Reject request failed", false, undefined);
        res.end(jsonString);
    }

    return next();
});


//---------------------------------Resource----------------------------------------------

server.post('/DVP/API/:version/ARDS/resource',authorization({resource:"ardsresource", action:"write"}), function (req, res, next) {

    var jsonString;
    var logKey = uuid();
    try {
        logger.info('+++++++++++++++LogKey: %s - Start post request - AddResource - req.body :: %j', logKey, req.body);

        var tenant = parseInt(req.user.tenant);
        var company = parseInt(req.user.company);
        resourceHandler.AddResource(logKey, tenant, company, req.body.ResourceId, req.user.iss, req.body.HandlingType).then(function (result) {

            logger.info('+++++++++++++++LogKey: %s - End post request - AddResource success :: %s', logKey, result);
            jsonString = messageFormatter.FormatMessage(undefined, "Add resource success", true, result);
            res.end(jsonString);

        }).catch(function (ex) {

            logger.error('+++++++++++++++LogKey: %s - End post request - AddResource failed :: %s', logKey, ex);
            if(ex.message) {
                jsonString = messageFormatter.FormatMessage(ex, "Add resource failed", false, undefined);
            }else{
                jsonString = messageFormatter.FormatMessage(undefined, ex, false, undefined);
            }
            res.end(jsonString);
        });

    } catch (ex) {

        logger.error('+++++++++++++++LogKey: %s - End post request - AddResource failed :: %s', logKey, ex);
        jsonString = messageFormatter.FormatMessage(ex, "Add resource failed", false, undefined);
        res.end(jsonString);
    }

    return next();
});

server.put('/DVP/API/:version/ARDS/resource',authorization({resource:"ardsresource", action:"write"}), function (req, res, next) {

    var jsonString;
    var logKey = uuid();
    try {
        logger.info('+++++++++++++++LogKey: %s - Start put request - SetResourceAttributes - req.body :: %j', logKey, req.body);

        var tenant = parseInt(req.user.tenant);
        var company = parseInt(req.user.company);
        resourceHandler.SetResourceAttributes(logKey, tenant, company, req.body.ResourceId, req.body.ResourceAttributeInfo).then(function (result) {

            logger.info('+++++++++++++++LogKey: %s - End put request - SetResourceAttributes success :: %s', logKey, result);
            jsonString = messageFormatter.FormatMessage(undefined, "Set resource attributes success", true, result);
            res.end(jsonString);

        }).catch(function (ex) {

            logger.error('+++++++++++++++LogKey: %s - End put request - SetResourceAttributes failed :: %s', logKey, ex);
            if(ex.message) {
                jsonString = messageFormatter.FormatMessage(ex, "Set resource attributes failed", false, undefined);
            }else{
                jsonString = messageFormatter.FormatMessage(undefined, ex, false, undefined);
            }
            res.end(jsonString);
        });

    } catch (ex) {

        logger.error('+++++++++++++++LogKey: %s - End put request - SetResourceAttributes failed :: %s', logKey, ex);
        jsonString = messageFormatter.FormatMessage(ex, "Set resource attributes failed", false, undefined);
        res.end(jsonString);
    }

    return next();
});

server.put('/DVP/API/:version/ARDS/resource/share',authorization({resource:"ardsresource", action:"write"}), function (req, res, next) {

    var jsonString;
    var logKey = uuid();
    try {
        logger.info('+++++++++++++++LogKey: %s - Start put request - ShareResource - req.body :: %j', logKey, req.body);

        var tenant = parseInt(req.user.tenant);
        var company = parseInt(req.user.company);
        resourceHandler.ShareResource(logKey, tenant, company, req.body.ResourceId, req.user.iss, req.body.HandlingType).then(function (result) {

            logger.info('+++++++++++++++LogKey: %s - End put request - ShareResource success :: %s', logKey, result);
            jsonString = messageFormatter.FormatMessage(undefined, "Share resource success", true, result);
            res.end(jsonString);

        }).catch(function (ex) {

            logger.error('+++++++++++++++LogKey: %s - End put request - ShareResource failed :: %s', logKey, ex);
            if(ex.message) {
                jsonString = messageFormatter.FormatMessage(ex, "Share resource failed", false, undefined);
            }else{
                jsonString = messageFormatter.FormatMessage(undefined, ex, false, undefined);
            }
            res.end(jsonString);
        });

    } catch (ex) {

        logger.error('+++++++++++++++LogKey: %s - End put request - ShareResource failed :: %s', logKey, ex);
        jsonString = messageFormatter.FormatMessage(ex, "Share resource failed", false, undefined);
        res.end(jsonString);
    }

    return next();
});

server.put('/DVP/API/:version/ARDS/resource/:resourceid/concurrencyslot',authorization({resource:"ardsresource", action:"write"}), function (req, res, next) {

    var jsonString;
    var logKey = uuid();

    try {
        logger.info('+++++++++++++++LogKey: %s - Start put request - UpdateSlotStatus - req.body :: %j', logKey, req.body);

        var tenant = parseInt(req.user.tenant);
        var company = parseInt(req.user.company);
        var resourceId = parseInt(req.params.resourceid);

        switch (req.body.State.toLowerCase()) {
            case 'reserved':

                resourceHandler.UpdateSlotStateReserved(logKey, tenant, company, resourceId, req.body.HandlingType, req.body.SlotId, req.body.SessionId, req.body.MaxReservedTime, req.body.MaxAfterWorkTime, req.body.MaxFreezeTime, req.body.TempMaxRejectCount, req.body.OtherInfo).then(function (result) {

                    logger.info('+++++++++++++++LogKey: %s - End put request - UpdateSlotStateReserved success :: %s', logKey, result);
                    jsonString = messageFormatter.FormatMessage(undefined, "Update slot state reserved success", true, result);
                    res.end(jsonString);

                }).catch(function (ex) {

                    logger.error('+++++++++++++++LogKey: %s - End put request - UpdateSlotStateReserved failed :: %s', logKey, ex);
                    if(ex.message) {
                        jsonString = messageFormatter.FormatMessage(ex, "Update slot state reserved failed", false, undefined);
                    }else{
                        jsonString = messageFormatter.FormatMessage(undefined, ex, false, undefined);
                    }
                    res.end(jsonString);
                });

                break;
            case 'connected':

                resourceHandler.UpdateSlotStateConnected(logKey, tenant, company, resourceId, req.body.HandlingType, req.body.SlotId, req.body.SessionId, 'inbound', req.body.OtherInfo).then(function (result) {

                    logger.info('+++++++++++++++LogKey: %s - End put request - UpdateSlotStateConnected success :: %s', logKey, result);
                    jsonString = messageFormatter.FormatMessage(undefined, "Update slot state connected success", true, result);
                    res.end(jsonString);

                }).catch(function (ex) {

                    logger.error('+++++++++++++++LogKey: %s - End put request - UpdateSlotStateConnected failed :: %s', logKey, ex);
                    if(ex.message) {
                        jsonString = messageFormatter.FormatMessage(ex, "Update slot state connected failed", false, undefined);
                    }else{
                        jsonString = messageFormatter.FormatMessage(undefined, ex, false, undefined);
                    }
                    res.end(jsonString);
                });

                break;
            case 'available':

                resourceHandler.UpdateSlotStateAvailable(logKey, tenant, company, resourceId, req.body.HandlingType, req.body.SlotId, '', req.body.OtherInfo, 'Available').then(function (result) {

                    logger.info('+++++++++++++++LogKey: %s - End put request - UpdateSlotStateAvailable success :: %s', logKey, result);
                    jsonString = messageFormatter.FormatMessage(undefined, "Update slot state available success", true, result);
                    res.end(jsonString);

                }).catch(function (ex) {

                    logger.error('+++++++++++++++LogKey: %s - End put request - UpdateSlotStateAvailable failed :: %s', logKey, ex);
                    if(ex.message) {
                        jsonString = messageFormatter.FormatMessage(ex, "Update slot state available failed", false, undefined);
                    }else{
                        jsonString = messageFormatter.FormatMessage(undefined, ex, false, undefined);
                    }
                    res.end(jsonString);
                });

                break;
            default :

                jsonString = messageFormatter.FormatMessage(undefined, 'Invalid Request State', false, undefined);
                res.end(jsonString);

                break;
        }

    } catch (ex) {

        logger.error('+++++++++++++++LogKey: %s - End put request - UpdateSlotStatus failed :: %s', logKey, ex);
        jsonString = messageFormatter.FormatMessage(ex, "Share resource failed", false, undefined);
        res.end(jsonString);
    }

    return next();
});

server.put('/DVP/API/:version/ARDS/resource/:resourceid/concurrencyslot/session/:sessionid',authorization({resource:"ardsresource", action:"write"}), function (req, res, next) {

    var jsonString;
    var logKey = uuid();
    try {
        logger.info('+++++++++++++++LogKey: %s - Start put request - UpdateSlotStateBySessionId - req.body :: %j', logKey, req.body);

        var tenant = parseInt(req.user.tenant);
        var company = parseInt(req.user.company);
        var resourceId = parseInt(req.params.resourceid);
        var direction = "inbound";
        if(req.query.direction)
            direction = req.query.direction;

        resourceHandler.UpdateSlotStateBySessionId(logKey, tenant, company, resourceId, req.body.RequestType, req.params.sessionid, req.body.State, req.body.Reason, req.body.OtherInfo, direction).then(function (result) {

            logger.info('+++++++++++++++LogKey: %s - End put request - UpdateSlotStateBySessionId success :: %s', logKey, result);
            jsonString = messageFormatter.FormatMessage(undefined, "Update slot state by sessionId success", true, result);
            res.end(jsonString);

        }).catch(function (ex) {

            logger.error('+++++++++++++++LogKey: %s - End put request - UpdateSlotStateBySessionId failed :: %s', logKey, ex);
            if(ex.message) {
                jsonString = messageFormatter.FormatMessage(ex, "Update slot state by sessionId failed", false, undefined);
            }else{
                jsonString = messageFormatter.FormatMessage(undefined, ex, false, undefined);
            }
            res.end(jsonString);
        });

    } catch (ex) {

        logger.error('+++++++++++++++LogKey: %s - End put request - UpdateSlotStateBySessionId failed :: %s', logKey, ex);
        jsonString = messageFormatter.FormatMessage(ex, "Update slot state by sessionId failed", false, undefined);
        res.end(jsonString);
    }

    return next();
});

server.put('/DVP/API/:version/ARDS/resource/:resourceid/state/:state/reason/:reason',authorization({resource:"ardsresource", action:"write"}), function (req, res, next) {

    var jsonString;
    var logKey = uuid();
    try {
        logger.info('+++++++++++++++LogKey: %s - Start put request - SetResourceState - req.body :: %j', logKey, req.body);

        var tenant = parseInt(req.user.tenant);
        var company = parseInt(req.user.company);
        var resourceId = parseInt(req.params.resourceid);

        resourceStatusMapper.SetResourceState(logKey, tenant, company, resourceId, req.user.iss, req.params.state, req.params.reason).then(function (result) {

            logger.info('+++++++++++++++LogKey: %s - End put request - SetResourceState success :: %s', logKey, result);
            jsonString = messageFormatter.FormatMessage(undefined, "Set resource state success", true, result);
            res.end(jsonString);

        }).catch(function (ex) {

            logger.error('+++++++++++++++LogKey: %s - End put request - SetResourceState failed :: %s', logKey, ex);
            if(ex.message) {
                jsonString = messageFormatter.FormatMessage(ex, "Set resource state failed", false, undefined);
            }else{
                jsonString = messageFormatter.FormatMessage(undefined, ex, false, undefined);
            }
            res.end(jsonString);
        });

    } catch (ex) {

        logger.error('+++++++++++++++LogKey: %s - End put request - SetResourceState failed :: %s', logKey, ex);
        jsonString = messageFormatter.FormatMessage(ex, "Set resource state failed", false, undefined);
        res.end(jsonString);
    }

    return next();
});

server.del('/DVP/API/:version/ARDS/resource/:resourceid/removesSharing/:handlingType',authorization({resource:"ardsresource", action:"delete"}), function (req, res, next) {

    var jsonString;
    var logKey = uuid();
    try {
        logger.info('+++++++++++++++LogKey: %s - Start delete request - RemoveShareResource - req.params :: %j', logKey, req.params);

        var tenant = parseInt(req.user.tenant);
        var company = parseInt(req.user.company);
        var resourceId = parseInt(req.params.resourceid);
        resourceHandler.RemoveShareResource(logKey, tenant, company, resourceId, req.params.handlingType).then(function (result) {

            logger.info('+++++++++++++++LogKey: %s - End delete request - RemoveShareResource success :: %s', logKey, result);
            jsonString = messageFormatter.FormatMessage(undefined, "Remove shared resource success", true, result);
            res.end(jsonString);

        }).catch(function (ex) {

            logger.error('+++++++++++++++LogKey: %s - End delete request - RemoveShareResource failed :: %s', logKey, ex);
            if(ex.message) {
                jsonString = messageFormatter.FormatMessage(ex, "Remove shared resource failed", false, undefined);
            }else{
                jsonString = messageFormatter.FormatMessage(undefined, ex, false, undefined);
            }
            res.end(jsonString);
        });

    } catch (ex) {

        logger.error('+++++++++++++++LogKey: %s - End delete request - RemoveShareResource failed :: %s', logKey, ex);
        jsonString = messageFormatter.FormatMessage(ex, "Remove shared resource failed", false, undefined);
        res.end(jsonString);
    }

    return next();
});

server.del('/DVP/API/:version/ARDS/resource/:resourceid',authorization({resource:"ardsresource", action:"delete"}), function (req, res, next) {

    var jsonString;
    var logKey = uuid();
    try {
        logger.info('+++++++++++++++LogKey: %s - Start delete request - RemoveResource - req.params :: %j', logKey, req.params);

        var tenant = parseInt(req.user.tenant);
        var company = parseInt(req.user.company);
        var resourceId = parseInt(req.params.resourceid);
        resourceHandler.RemoveResource(logKey, tenant, company, resourceId).then(function (result) {

            logger.info('+++++++++++++++LogKey: %s - End delete request - RemoveResource success :: %s', logKey, result);
            jsonString = messageFormatter.FormatMessage(undefined, "Remove resource success", true, result);
            res.end(jsonString);

        }).catch(function (ex) {

            logger.error('+++++++++++++++LogKey: %s - End delete request - RemoveResource failed :: %s', logKey, ex);
            if(ex.message) {
                jsonString = messageFormatter.FormatMessage(ex, "Remove resource failed", false, undefined);
            }else{
                jsonString = messageFormatter.FormatMessage(undefined, ex, false, undefined);
            }
            res.end(jsonString);
        });

    } catch (ex) {

        logger.error('+++++++++++++++LogKey: %s - End delete request - RemoveResource failed :: %s', logKey, ex);
        jsonString = messageFormatter.FormatMessage(ex, "Remove resource failed", false, undefined);
        res.end(jsonString);
    }

    return next();
});



server.listen(config.Host.Port, function () {
    console.log('%s listening at %s', server.name, server.url);
});