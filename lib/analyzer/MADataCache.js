/**
 * Created by song on 2016/1/4.
 */
var uuid = require('node-uuid');
var MAThriftClient = require('./MAThriftClient.js');
var logger = require('../mlogger/mlogger.js');

var fs = require('fs');
var path = require('path');
var dataDirPath = path.join(__dirname, '../data');
var dataFilePath = dataDirPath + "/data.db";
if (!fs.existsSync(dataDirPath)) {
    fs.mkdirSync(dataDirPath);
}

var sqlite3 = require('sqlite3');
sqlite3.verbose();

function AnalyzerDataCache(thriftClient) {
    this.db = null;
    this.thriftClient = thriftClient;
    this.initDb = function () {
        var self = this;
        self.db = new sqlite3.Database(dataFilePath, sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE,
            function (err) {
                if (err) {
                    logger.warn('FAIL on creating database ' + err);
                    self.db = null;
                } else {
                    self.db.run("CREATE TABLE IF NOT EXISTS mhome_data_temp " +
                        "(rowkey VARCHAR2(64), ts DATETIME, content VARCHAR2(512))",
                        function (err) {
                            if (err) {
                                logger.warn('FAIL on creating table ' + err);
                                self.db = null;
                            }
                        });
                }
            });
    };

    this.addDataToDb = function (data) {
        var self = this;
        if (self.db === null)
            return;
        logger.warn("addDataToDb: " + data);
        self.db.run("INSERT INTO mhome_data_temp (rowkey, ts, content) " +
            "VALUES (?, ?, ?);",
            [uuid.v4(), new Date(), data],
            function (error) {
                if (error) {
                    logger.warn('FAIL on add ' + error);
                }
            });
    };

    this.sendToDb = function () {
        var self = this;
        if (self.db !== null) {
            self.db.each("SELECT * FROM mhome_data_temp limit 0,99",
                function (err, row) {
                    if (err) {
                        logger.warn('FAIL to retrieve row ' + err);
                    } else {
                        logger.warn("sendToDb: " + row.content);
                        self.thriftClient.sendMsg(row.content, function (err, response) {
                            if (!err) {
                                self.db.run("DELETE FROM mhome_data_temp WHERE rowkey = ?;",
                                    [row.rowkey],
                                    function (err) {
                                        if (err) {
                                            logger.warn('FAIL to delete ' + err);
                                        } else {
                                        }
                                    });
                            }
                        });
                    }
                },
                function (err, row) {
                    //logger.info('select mhome_data_temp OK');
                });

        } else {
            self.initDb();
        }
    };

    this.init = function () {
        var self = this;
        self.initDb();
        setInterval(function () {
            self.sendToDb();
        }, 30000);
    };
    this.add = function (data) {
        var self = this;
        self.addDataToDb(data);
    };

}

module.exports = AnalyzerDataCache;