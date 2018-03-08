'use strict';
var util = require('util');
var lodash = require('lodash');
var VirtualDevice = require('./../virtual-device').VirtualDevice;
var logger = require('../mlogger/mlogger.js');
var thrift = require('thrift');
var MADataService = require('./MADataService.js');
var ThriftTransports = require('thrift/lib/nodejs/lib/thrift/transport');
var ThriftProtocols = require('thrift/lib/nodejs/lib/thrift/protocol');
var transport = ThriftTransports.TBufferedTransport();
var protocol = ThriftProtocols.TBinaryProtocol();
var MAThriftClient = require('./MAThriftClient.js');
var MADataCache = require('./MADataCache.js');
var OPERATION_SCHEMAS = {
  getData: {
    "type": "object",
    "properties": {}
  },
  getItem: {
    "type": "object",
    "properties": {}
  },
  putData: {
    "type": "object",
    "properties": {}
  },
  updateDevice: {
    "type": "object",
    "properties": {}
  }

};
var ONE_HOUR_MS = 1000 * 60 * 60;
var ONE_DAY_MS = ONE_HOUR_MS * 24;

var rebuild = function (dataType, data) {
  var date = new Date();
  var retDataArray = [];
  if ("dailyReport" === dataType) {
    var curHour = date.getHours();
    retDataArray = new Array(curHour + 1);
    if (util.isArray(data)) {
      data.forEach(function (item, index) {
        var data = new Date(item.timestamp);
        var hour = data.getHours();
        retDataArray[hour] = item;
      })
    }
  }
  else if ("monthlyReport" === dataType) {
    var curDay = date.getDate();
    retDataArray = new Array(curDay);
    if (util.isArray(data)) {
      data.forEach(function (item, index) {
        var data = new Date(item.timestamp);
        var day = data.getDate();
        retDataArray[day - 1] = item;
      })
    }
  }
  else if ("yearlyReport" === dataType) {
    var curMoth = date.getMonth();
    retDataArray = new Array(curMoth + 1);
    if (util.isArray(data)) {
      data.forEach(function (item, index) {
        var data = new Date(item.timestamp);
        var month = data.getMonth();
        retDataArray[month - 1] = item;
      })
    }
  }
  else if ("all" === dataType) {
    retDataArray = data;
  }
  return retDataArray;
};
var buildVirtualData = function (message, callback) {
  var retData = [];
  var curDate = new Date();
  if ("9f78e9e6-4a01-4fbf-8c4e-6009ddd3ffff" === message.uuid
    || "9f78e9e6-4a01-4fbf-8c4e-6009ddd3fccc" === message.uuid) {
    if ("dailyReport" === message.dataType) {
      var curHour = curDate.getHours();
      curDate.setHours(0, 0, 0, 0);
      for (var h = 0; h <= curHour; ++h) {
        curDate.setHours(h + 8, 0, 0, 0);
        retData.push({
          "timestamp": curDate.toISOString(),
          "type": "040B01000004",
          "dataType": message.dataType,
          "userId": "a9d4a734-598f-4c63-ba4f-f60efb3d177f",
          "uuid": message.uuid,
          "timezone": 28800000,
          "analyzerId": "1",
          "currentPower": 1000 + Math.floor(Math.random() * 200),
          "totalEnergy": Math.random() * 10
        });
      }

    }
    else if ("monthlyReport" === message.dataType) {
      var curDay = curDate.getDate();
      curDate.setHours(0, 0, 0, 0);
      curDate.setHours(curDate.getHours() + 8, 0, 0, 0);
      curDate.setMonth(curDate.getMonth(), 1);
      for (var d = 1; d <= curDay; ++d) {
        curDate.setMonth(curDate.getMonth(), d);
        retData.push({
          "timestamp": curDate.toISOString(),
          "type": "040B01000004",
          "dataType": message.dataType,
          "userId": "a9d4a734-598f-4c63-ba4f-f60efb3d177f",
          "uuid": message.uuid,
          "timezone": 28800000,
          "analyzerId": "1",
          "currentPower": 1000 + Math.floor(Math.random() * 200),
          "totalEnergy": 14 + Math.random() * 10
        });
      }
    }
    else if ("yearlyReport" === message.dataType) {
      var curMoth = curDate.getMonth();
      curDate.setHours(0, 0, 0, 0);
      curDate.setHours(curDate.getHours() + 8, 0, 0, 0);
      curDate.setMonth(0, 1);
      for (var m = 0; m <= curMoth; ++m) {
        curDate.setMonth(m, 1);
        retData.push({
          "timestamp": curDate.toISOString(),
          "type": "040B01000004",
          "dataType": message.dataType,
          "userId": "a9d4a734-598f-4c63-ba4f-f60efb3d177f",
          "uuid": message.uuid,
          "timezone": 28800000,
          "analyzerId": "1",
          "currentPower": 1000 + Math.floor(Math.random() * 200),
          "totalEnergy": 100 + Math.random() * 100
        });
      }
    }
    else {
      retData.push({
        "timestamp": "2017-01-01T00:00:00.974Z",
        "type": "040B01000004",
        "dataType": message.dataType,
        "userId": "a9d4a734-598f-4c63-ba4f-f60efb3d177f",
        "uuid": message.uuid,
        "timezone": 28800000,
        "analyzerId": "1",
        "currentPower": 1000 + Math.floor(Math.random() * 200),
        "totalEnergy": 1000 + Math.random() * 100
      });
    }
  }
  else {
    if ("dailyReport" === message.dataType) {
      var curHour = curDate.getHours();
      curDate.setHours(0, 0, 0, 0);
      for (var h = 0; h <= curHour; ++h) {
        curDate.setHours(h + 8, 0, 0, 0);
        retData.push({
          "timestamp": curDate.toISOString(),
          "type": "040B01000005",
          "dataType": message.dataType,
          "userId": "a9d4a734-598f-4c63-ba4f-f60efb3d177f",
          "uuid": message.uuid,
          "timezone": 28800000,
          "analyzerId": "1",
          "pacToGrid": 1000 + Math.floor(Math.random() * 200),
          "eDisChargeTotal": 567 + Math.random() * 100,
          "eChargeTotal": 589 + Math.random() * 100,
          "pacToUser": 1000 + Math.floor(Math.random() * 200),
          "eToGridTotal": 1345 + Math.random() * 100
        });
      }

    }
    else if ("monthlyReport" === message.dataType) {
      var curDay = curDate.getDate();
      curDate.setHours(0, 0, 0, 0);
      curDate.setHours(curDate.getHours() + 8, 0, 0, 0);
      curDate.setMonth(curDate.getMonth(), 1);
      for (var d = 1; d <= curDay; ++d) {
        curDate.setMonth(curDate.getMonth(), d);
        retData.push({
          "timestamp": curDate.toISOString(),
          "type": "040B01000005",
          "dataType": message.dataType,
          "userId": "a9d4a734-598f-4c63-ba4f-f60efb3d177f",
          "uuid": message.uuid,
          "timezone": 28800000,
          "analyzerId": "1",
          "pacToGrid": 1000 + Math.floor(Math.random() * 200),
          "eDisChargeTotal": 567 + Math.random() * 100,
          "eChargeTotal": 589 + Math.random() * 100,
          "pacToUser": 1000 + Math.floor(Math.random() * 200),
          "eToGridTotal": 1345 + Math.random() * 100
        });
      }
    }
    else if ("yearlyReport" === message.dataType) {
      var curMoth = curDate.getMonth();
      curDate.setHours(0, 0, 0, 0);
      curDate.setHours(curDate.getHours() + 8, 0, 0, 0);
      curDate.setMonth(0, 1);
      for (var m = 0; m <= curMoth; ++m) {
        curDate.setMonth(m, 1);
        retData.push({
          "timestamp": curDate.toISOString(),
          "type": "040B01000005",
          "dataType": message.dataType,
          "userId": "a9d4a734-598f-4c63-ba4f-f60efb3d177f",
          "uuid": message.uuid,
          "timezone": 28800000,
          "analyzerId": "1",
          "pacToGrid": 1000 + Math.floor(Math.random() * 200),
          "eDisChargeTotal": 567 + Math.random() * 100,
          "eChargeTotal": 589 + Math.random() * 100,
          "pacToUser": 1000 + Math.floor(Math.random() * 200),
          "eToGridTotal": 1345 + Math.random() * 100
        });
      }
    }
    else {
      retData.push({
        "timestamp": "2017-01-01T00:00:00.974Z",
        "type": "040B01000005",
        "dataType": message.dataType,
        "userId": "a9d4a734-598f-4c63-ba4f-f60efb3d177f",
        "uuid": message.uuid,
        "timezone": 28800000,
        "analyzerId": "1",
        "pacToGrid": 1000 + Math.floor(Math.random() * 200),
        "eDisChargeTotal": 567 + Math.random() * 100,
        "eChargeTotal": 589 + Math.random() * 100,
        "pacToUser": 1000 + Math.floor(Math.random() * 200),
        "eToGridTotal": 1345 + Math.random() * 100
      });
    }
  }
  callback(retData);
};

/**
 * @constructor
 * */
function Analyzer(conx, uuid, token, configurator) {
  this.thriftClient = null;
  this.dataCache = null;
  VirtualDevice.call(this, conx, uuid, token, configurator);
}

util.inherits(Analyzer, VirtualDevice);

/**
 * 设备管理器初始化，将系统已经添加的设备实例化并挂载到Meshblu网络
 * */
Analyzer.prototype.init = function () {
  var self = this;
  var responseMsg = {
    payload: {
      code: 206000,
      message: "failed"
    }
  };

  var serverThrift = thrift.createServer(MADataService, {
    onMessage: function (message, callback) {
      logger.debug("AnalyzerPlugin.onMessage: " + JSON.stringify(message));
      self.message(JSON.parse(message), function (responseMessage) {
        try {
          logger.debug("AnalyzerPlugin.onMessage.response: " + JSON.stringify(responseMessage));
          if (responseMessage.error) {
            callback(null, responseMsg);
          } else {
            callback(null, JSON.stringify(responseMessage));
          }
        } catch (err) {
          logger.error(206000, {"info": "AnalyzerPlugin.serverThrift.onMessage: " + JSON.stringify(err)});
          callback(null, responseMsg);
        }
      });
    }
  }, {
    transport: transport,
    protocol: protocol
  });

  serverThrift.on('error', function (err) {
    logger.error(206001, {"info": "Analyzer thrift server error: " + JSON.stringify(err)});
  });
  serverThrift.listen(self.configurator.getConf("self.listen_port"));
  self.thriftClient = new MAThriftClient(self.configurator);
  self.thriftClient.init();
  self.dataCache = new MADataCache(self.thriftClient);
  self.dataCache.init();
  self.isInitCompleted = true;

};

/**
 * 远程RPC回调函数
 * @callback onMessage~getData
 * @param {object} response:
 * {
 *      "payload":
 *      {
 *          "code":{number},
 *          "message":{string},
 *          "data":null
 *      }
 * }
 */
/**
 * 获取分析结果
 * @param {object} message:消息体
 * @param {onMessage~getData} peerCallback: 远程RPC回调函数
 * */
Analyzer.prototype.getData = function (message, peerCallback) {
  var self = this;
  var thriftMsg = {};
  var responseMessage = {retCode: 200, description: "Success.", data: {}};
  if (!util.isNullOrUndefined(message.method)) {
    thriftMsg = message;
  } else {
    thriftMsg = {
      devices: self.configurator.getConf("self.uuid"),
      payload: {
        method: 'getData',
        parameters: []
      }
    };
    thriftMsg.payload.parameters[0] = {payload: message};
  }
  //for test/////////////////////////////
  if ("9f78e9e6-4a01-4fbf-8c4e-6009ddd3ffff" === message.uuid
    || "e871dc3f-f4d5-4542-8df7-c3bef511bfff" === message.uuid
    || "9f78e9e6-4a01-4fbf-8c4e-6009ddd3fccc" === message.uuid
    || "e871dc3f-f4d5-4542-8df7-c3bef511beec" === message.uuid
  ) {
    //inverter
    buildVirtualData(message, function (retData) {
      responseMessage.retCode = 200;
      responseMessage.description = "success";
      responseMessage.data = retData;
      logger.debug(responseMessage);
      peerCallback(responseMessage);
    })
  }
  else {
    self.thriftClient.sendMsg(JSON.stringify(thriftMsg), function (err, response) {
      logger.debug(response);
      if (response) {
        var resp = JSON.parse(response);
        responseMessage.retCode = resp.payload.code;
        responseMessage.description = resp.payload.message;
        responseMessage.data = resp.payload.data;
      }
      logger.debug(responseMessage);
      peerCallback(responseMessage);
    });
  }
  ///////////////////////////////////////

};


/**
 * 远程RPC回调函数
 * @callback onMessage~deleteDevice
 * @param {object} response:
 * {
 *      "payload":
 *      {
 *          "code":{number},
 *          "message":{string},
 *          "data":null
 *      }
 * }
 */
/**
 * 获取分析结果
 * @param {object} message:消息体
 * @param {onMessage~deleteDevice} peerCallback: 远程RPC回调函数
 * */
Analyzer.prototype.deleteDevice = function (message, peerCallback) {
  var self = this;
  var thriftMsg = {};
  var responseMessage = {retCode: 200, description: "Success.", data: {}};
  if (!util.isNullOrUndefined(message.method)) {
    thriftMsg = message;
  } else {
    thriftMsg = {
      devices: self.configurator.getConf("self.uuid"),
      payload: {
        method: 'deleteDevice',
        parameters: []
      }
    };
    thriftMsg.payload.parameters[0] = message;
  }

  self.thriftClient.sendMsg(JSON.stringify(thriftMsg), function (err, response) {
    if (response) {
      var resp = JSON.parse(response);
      responseMessage.retCode = resp.payload.code;
      responseMessage.description = resp.payload.message;
      responseMessage.data = resp.payload.data;
    }
    peerCallback(responseMessage);
  });
};


/**
 * 远程RPC回调函数
 * @callback onMessage~getItem
 * @param {object} response:
 * {
 *      "payload":
 *      {
 *          "code":{number},
 *          "message":{string},
 *          "data":null
 *      }
 * }
 */
/**
 * 获取分析结果
 * @param {object} message:消息体
 * @param {onMessage~getItem} peerCallback: 远程RPC回调函数
 * */
Analyzer.prototype.getItem = function (message, peerCallback) {
  var self = this;
  var thriftMsg = {};
  var responseMessage = {retCode: 200, description: "Success.", data: {}};
  if (!util.isNullOrUndefined(message.method)) {
    thriftMsg = message;
  } else {
    thriftMsg = {
      devices: self.configurator.getConf("self.uuid"),
      payload: {
        method: 'getItem',
        parameters: []
      }
    };
    thriftMsg.payload.parameters[0] = message;
  }

  self.thriftClient.sendMsg(JSON.stringify(thriftMsg), function (err, response) {
    if (response) {
      var resp = JSON.parse(response);
      responseMessage.retCode = resp.payload.code;
      responseMessage.description = resp.payload.message;
      responseMessage.data = resp.payload.data;
    }
    peerCallback(responseMessage);
  });
};

/**
 * 远程RPC回调函数
 * @callback onMessage~putData
 * @param {object} response:
 * {
 *      "payload":
 *      {
 *          "code":{number},
 *          "message":{string},
 *          "data":null
 *      }
 * }
 */
/**
 * 添加原始采集数据
 * @param {object} message:消息体
 * @param {onMessage~putData} peerCallback: 远程RPC回调函数
 * */
Analyzer.prototype.putData = function (message, peerCallback) {
  var self = this;
  logger.debug("AnalyzerPlugin.putData: " + JSON.stringify(message));
  var responseMessage = {retCode: 200, description: "Success.", data: {}};
  var thriftMsg = {};
  if (!util.isNullOrUndefined(message.method)) {
    thriftMsg = message;
  } else {
    thriftMsg = {
      devices: self.configurator.getConf("self.uuid"),
      payload: {
        method: 'putData',
        parameters: []
      }
    };
    thriftMsg.payload.parameters[0] = {payload: message};
  }
  logger.debug(JSON.stringify(thriftMsg));
  self.thriftClient.sendMsg(JSON.stringify(thriftMsg), function (err, response) {
    if (err) {
      logger.warn("<***>Put data failed, save the data into data cache.");
      logger.warn(err);
      self.dataCache.add(JSON.stringify(thriftMsg));
    }
    else {
      logger.debug(response);
      var resp = JSON.parse(response);
      responseMessage.retCode = resp.payload.code;
      responseMessage.description = resp.payload.message;
      responseMessage.data = resp.payload.data;
    }
    peerCallback(responseMessage);
  });

  /*var client = new kafka.Client(sysConf.MAnalyzer.remoteKafkaHost + ":" + sysConf.MAnalyzer.remoteKafkaPort),
   producer = new kafka.Producer(client
   //, { requireAcks: 0 }
   );

   producer.on('ready', function () {
   var payloads = [
   { topic: 'mhome', messages: JSON.stringify(message),  partition: 0 }
   ];

   producer.send(payloads, function (err, data) {

   var responseMsg = {
   "payload": {
   "code": 200,
   "message": "success",
   "data": []
   }
   };
   peerCallback(responseMsg);
   }
   )
   }
   );

   producer.on('error', function (err) {
   logger.error(206001, {"info": "M-Analyzer putData error: " + JSON.stringify(err)});
   })
   */
};


/**
 * 远程RPC回调函数
 * @callback onMessage~updateDevice
 * @param {object} response:
 * {
 *      "payload":
 *      {
 *          "code":{number},
 *          "message":{string},
 *          "data":null
 *      }
 * }
 */
/**
 * 获取分析结果
 * @param {object} message:消息体
 * @param {onMessage~updateDevice} peerCallback: 远程RPC回调函数
 * */
Analyzer.prototype.updateDevice = function (message, peerCallback) {
  var self = this;
  var thriftMsg = {};
  if (!util.isNullOrUndefined(message.method)) {
    thriftMsg = message;
  } else {
    thriftMsg = {
      devices: self.configurator.getConf("self.uuid"),
      payload: {
        method: 'updateDevice',
        parameters: []
      }
    };
    thriftMsg.payload.parameters[0] = message;
  }

  self.thriftClient.sendMsg(JSON.stringify(thriftMsg), function (err, response) {
    peerCallback(JSON.parse(response));
  });
};

module.exports = {
  Service: Analyzer,
  OperationSchemas: OPERATION_SCHEMAS
};