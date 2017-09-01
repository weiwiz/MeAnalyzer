/**
 * Created by song on 2016/1/4.
 */


var thrift = require('thrift');
var async = require('async');
var MADataService = require('./MADataService.js');
var ThriftTransports = require('thrift/lib/nodejs/lib/thrift/transport');
var ThriftProtocols = require('thrift/lib/nodejs/lib/thrift/protocol');
var transport = ThriftTransports.TBufferedTransport();
var protocol = ThriftProtocols.TBinaryProtocol();
var logger = require('../mlogger/mlogger.js');
var configurator = require('../configurator');

function AnalyzerThriftClient(configurator) {
    this.initFlag = false;
    this.connectFlag = false;
    this.client = null;
    this.configurator = configurator;
    this.init = function () {
        var self = this;
        self.connect();
        self.initFlag = true;
    };
    this.connect = function () {
        var self = this;
        self.connectFlag = false;
        try {
            async.waterfall([
                function (innerCallback) {

                }
            ],function (error, result) {

            });
            var connection = thrift.createConnection(
                self.configurator.getConf("self.remote_host"), self.configurator.getConf("self.remote_port"), {
                    transport: transport,
                    protocol: protocol
                });

            connection.on('error', function (err) {
                console.log("M-Analyzer server connect error:" + JSON.stringify(err));
                self.connectFlag = false;
            });

            connection.on('timeout', function () {
                console.log("M-Analyzer server connect timeout");
                self.connectFlag = false;
            });

            connection.on('connect', function () {
                console.log("connect to M-Analyzer");
                self.client = thrift.createClient(MADataService, connection);
                self.connectFlag = true;
            });

            connection.on('secureConnect', function () {
                console.log("connect to M-Analyzer");
                self.client = thrift.createClient(MADataService, connection);
                self.connectFlag = true;
            });
        } catch (error) {
            console.log("M-Analyzer server createConnection error:" + JSON.stringify(error));
        }
        return self.connectFlag;
    };

    this.sendMsg = function (message, callback) {
        var self = this;
        if (!self.connectFlag) {
            self.connect();
        }
        if (!self.connectFlag) {
            var response = {
                payload: {
                    code: 206003,
                    message: "M-Analyzer server sendMsg error: connect is failed",
                    data: []
                }
            };
            var err = {
                "code": "EPIPE",
                "errno": "EPIPE",
                "syscall": "connect"
            };
            callback(err, JSON.stringify(response));
        }
        else if(self.client){
            self.client.onMessage(message, function (err, response) {
                if (err) {
                    response = {
                        payload: {
                            code: 206003,
                            message: "" + err,
                            data: []
                        }
                    };
                    response = JSON.stringify(response);
                }
                callback(err, response);
            });
        }
    }

}

module.exports = AnalyzerThriftClient;
