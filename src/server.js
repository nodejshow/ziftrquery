var rpc = require('node-json-rpc');
    log4js = require('log4js'),
    MongoClient = require('mongodb').MongoClient,
    async = require("async");

var options = {
    port: process.env.rpc_port,
    host: process.env.rpc_host,
    path: '/',
    strict: true,
    login: process.env.rpc_user,
    hash: process.env.rpc_pass,
};

var logger = log4js.getLogger();

var start = 0,
    counter = 0;

var service = {
        getblockcount: function(callback){
            var client = new rpc.Client(options);
            client.call({"method": "getblockcount", "params": []}, function (err, result) {
                if(err){
                    logger.error(err);
                }else{
                    callback(err, result.result);    
                }
            });
        },
        getblockhash: function(height, callback){
            var client = new rpc.Client(options);
            client.call({"method": "getblockhash", "params": [height]}, function (err, result) {
                if(err){
                    logger.error(err);
                }else{
                    callback(err, result.result);    
                }
            });
        },
        getBlock: function(hash, callback){
            var client = new rpc.Client(options);
            client.call({"method": "getblock", "params": [hash]}, function (err, result) {
                if(err){
                    logger.error(err);
                }else{
                    callback(err, result.result);    
                }
            });
        },
        getrawtransaction: function(tx, callback){
            var client = new rpc.Client(options);
            client.call({"method": "getrawtransaction", "params": [tx, 1]}, function (err, result) {
                if(err){
                    logger.error(err);
                }else{
                    callback(err, result.result);    
                }
            });
        },
        decoderawtransaction: function(raw, callback){
            var client = new rpc.Client(options);
            client.call({"method": "decoderawtransaction", "params": [raw]}, function (err, result) {
                if(err){
                    logger.error(err);
                }else{
                    callback(err, result.result);    
                }
            });
        }
    };
MongoClient.connect(process.env.mongodb, function(err, db) {
    if(err){
        logger.error(err); 
    }else{
        var blockchain_collect = function(){
                logger.info("Start collecting block in chain.");
                collect();
            },
            collectTx = function(tx, callback){
                async.each(tx, function(txid, callback){
                    service.getrawtransaction(txid, function(err, raw){
                        //raw._id = raw.txid;
                        dbTx.insert([raw], function(err, result){
                            if(err){
                                logger.error(err);
                            }else{
                                callback();    
                            }
                            
                        });
                    });
                },function(err,result){
                    callback();
                });
            },
            collectBlock = function(hash, callback){
                service.getBlock(hash, function(err, block){
                    logger.info(block.height + " / " + counter)
                    //block._id = block.height;
                    dbBlock.insert([block], function(err, result){
                        if(err){
                            logger.error(err);
                        }else{
                            collectTx(block.tx, function(){
                                if(block.hasOwnProperty("nextblockhash")){
                                    collectBlock(block.nextblockhash, function(){
                                        callback();
                                    }); 
                                }else{
                                    logger.info("Query done!");
                                    callback();    
                                }
                            });    
                        }
                    });
                });
            },
            collect = function(){
                service.getblockcount(function(err, height){
                    counter = height;
                    var diff = counter - start;

                    if(diff > 0){
                        service.getblockhash(start, function(err, hash){
                            collectBlock(hash, function(){
                                setTimeout(collect, 1000);
                            });
                        });    
                    }else{
                        logger.info("No new block to prosses!");
                        setTimeout(collect, 1000);
                    }
                    
                });
            }


        var dbBlock = db.collection('block');
        var dbTx = db.collection('tx');

        logger.info("Connected correctly to server");

        dbBlock.find().sort({_id:-1}).limit(1).toArray(function(err, docs) {
            if(docs && docs[0] && docs[0].height)
                start = docs[0].height;
            else
                start = 0;

            blockchain_collect();
        });
    }
});