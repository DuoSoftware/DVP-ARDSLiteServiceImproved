﻿module.exports = {
	"Redis":
	{
		"mode":"SYS_REDIS_MODE",
		"ip": "SYS_REDIS_HOST",
		"port": "SYS_REDIS_PORT",
		"user": "SYS_REDIS_USER",
		"password": "SYS_REDIS_PASSWORD",
		"sentinels":{
			"hosts": "SYS_REDIS_SENTINEL_HOSTS",
			"port":"SYS_REDIS_SENTINEL_PORT",
			"name":"SYS_REDIS_SENTINEL_NAME"
		}

	},
	"Services" : {
		"accessToken": "HOST_TOKEN",
		"routingServiceHost": "SYS_ARDSLITEROUTINGENGINE_HOST",
		"routingServicePort": "SYS_ARDSLITEROUTINGENGINE_PORT",
		"routingServiceVersion": "SYS_ARDSLITEROUTINGENGINE_VERSION",
		"resourceServiceHost": "SYS_RESOURCESERVICE_HOST",
		"resourceServicePort": "SYS_RESOURCESERVICE_PORT",
		"resourceServiceVersion": "SYS_RESOURCESERVICE_VERSION",
		"notificationServiceHost": "SYS_NOTIFICATIONSERVICE_HOST",
		"notificationServicePort": "SYS_NOTIFICATIONSERVICE_PORT",
		"notificationServiceVersion": "SYS_NOTIFICATIONSERVICE_VERSION",
		"ardsMonitoringServiceHost": "SYS_ARDSMONITORING_HOST",
		"ardsMonitoringServicePort": "SYS_ARDSMONITORING_PORT",
		"ardsMonitoringServiceVersion": "SYS_ARDSMONITORING_VERSION",
		"cronurl": "SYS_SCHEDULEWORKER_HOST",//ardsmonitoring.app.veery.cloud
		"cronport": "SYS_SCHEDULEWORKER_PORT",
		"cronversion": "SYS_SCHEDULEWORKER_VERSION"
	},
	"Host": {
		"LBIP":"LB_FRONTEND",
		"LBPort":"LB_PORT",
		"Port": "HOST_ARDSLITESERVICE_PORT",
		"Version": "HOST_VERSION",
		"UseMsgQueue": "HOST_USE_MSG_QUEUE"
	},
	"DB": {
	    "Type": "SYS_DATABASE_TYPE",
	    "User": "SYS_DATABASE_POSTGRES_USER",
	    "Password": "SYS_DATABASE_POSTGRES_PASSWORD",
	    "Port": "SYS_SQL_PORT",
	    "Host": "SYS_DATABASE_HOST",
	    "Database": "SYS_DATABASE_POSTGRES_USER"
	},
	"Security":
	{

		"ip": "SYS_REDIS_HOST",
		"port": "SYS_REDIS_PORT",
		"user": "SYS_REDIS_USER",
		"password": "SYS_REDIS_PASSWORD",
		"mode":"SYS_REDIS_MODE",
		"sentinels":{
			"hosts": "SYS_REDIS_SENTINEL_HOSTS",
			"port":"SYS_REDIS_SENTINEL_PORT",
			"name":"SYS_REDIS_SENTINEL_NAME"
		}

	},
	"RabbitMQ":
	{
		"ip": "SYS_RABBITMQ_HOST",
		"port": "SYS_RABBITMQ_PORT",
		"user": "SYS_RABBITMQ_USER",
		"password": "SYS_RABBITMQ_PASSWORD",
		"vhost":"SYS_RABBITMQ_VHOST"
	}
};
