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
        logger.info('+++++++++++++++LogKey: %s - Start get request - SetRequestServer - req.params :: %j', logKey, req.params);

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
        logger.info('+++++++++++++++LogKey: %s - Start delete request - SetRequestServer - req.params :: %j', logKey, req.params);

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



server.listen(config.Host.Port, function () {
    console.log('%s listening at %s', server.name, server.url);
});