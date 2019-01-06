var mongoose = require('mongoose')
  , db = require('../lib/database')
  , Tx = require('../models/tx')  
  , Address = require('../models/address')  
  , Richlist = require('../models/richlist')  
  , Stats = require('../models/stats')  
  , settings = require('../lib/settings')
  , fs = require('fs');

var mode = 'update';
var database = 'index';
var maxcnt = -1;
var startidx = 0;
var endidx = 0;

// displays usage and exits
function usage() {
  console.log('Usage: node scripts/sync.js [database] [mode]');
  console.log('');
  console.log('database: (required)');
  console.log('index [mode] Main index: coin info/stats, transactions & addresses');
  console.log('market       Market data: summaries, orderbooks, trade history & chartdata')
  console.log('');
  console.log('mode: (required for index database only)');
  console.log('update       Updates index from last sync to current block');
  console.log('check        checks index for (and adds) any missing transactions/addresses');
  console.log('reindex      Clears index then resyncs from genesis to current block');
  console.log('richlist     only richlist update');
  console.log('');
  console.log('notes:'); 
  console.log('* \'current block\' is the latest created block when script is executed.');
  console.log('* The market database only supports (& defaults to) reindex mode.');
  console.log('* If check mode finds missing data(ignoring new data since last sync),'); 
  console.log('  index_timeout in settings.json is set too low.')
  console.log('');
  process.exit(0);
}

// check options
//console.log(process.argv.length);
if (process.argv[2] == 'index') {
  if (process.argv.length <= 3) {
    usage();
  } else {
    //if (process.argv.length > 4){
      if (process.argv[3] != "check"){
        startidx = 1;
        maxcnt = parseInt(process.argv[4]);
      }else{
        startidx = parseInt(process.argv[4]);
        maxcnt = parseInt(process.argv[5]);
      }
      if (startidx < 1 || isNaN(startidx)){ startidx = 1; }
      if (maxcnt < 0 || isNaN(maxcnt)){ maxcnt = -1; }
      //console.log("%s, %s", startidx, maxcnt);
    //}
    switch(process.argv[3])
    {
    case 'update':
      mode = 'update';
      break;
    case 'check':
      mode = 'check';
      break;
    case 'reindex':
      mode = 'reindex';
      break;
    case 'richlist':
      mode = 'richlist';
      break;
    default:
      usage();
    }
  }
} else if (process.argv[2] == 'market'){
  database = 'market';
} else {
  usage();
}

function create_lock(cb) {
  if ( database == 'index' ) {
    var fname = './tmp/' + database + '.pid';
    fs.appendFile(fname, process.pid, function (err) {
      if (err) {
        console.log("Error: unable to create %s", fname);
        process.exit(1);
      } else {
        return cb();
      }
    });
  } else {
    return cb();
  }
}

function remove_lock(cb) {
  if ( database == 'index' ) {
    var fname = './tmp/' + database + '.pid';
    fs.unlink(fname, function (err){
      if(err) {
        console.log("unable to remove lock: %s", fname);
        process.exit(1);
      } else {
        return cb();
      }
    });
  } else {
    return cb();
  }  
}

function is_locked(cb) {
  if ( database == 'index' ) {
    var fname = './tmp/' + database + '.pid';
    fs.exists(fname, function (exists){
      if(exists) {
        return cb(true);
      } else {
        return cb(false);
      }
    });
  } else {
    return cb();
  } 
}

function exit() {
  remove_lock(function(){
    mongoose.disconnect();
    process.exit(0);
  });
}

var dbString = 'mongodb://' + settings.dbsettings.user;
dbString = dbString + ':' + settings.dbsettings.password;
dbString = dbString + '@' + settings.dbsettings.address;
dbString = dbString + ':' + settings.dbsettings.port;
dbString = dbString + '/' + settings.dbsettings.database;

is_locked(function (exists) {
  if (exists) {
    console.log("Script already running..");
    process.exit(0);
  } else {
    create_lock(function (){
      console.log("script launched with pid: " + process.pid);
      mongoose.connect(dbString, function(err) {
        if (err) {
          console.log('Unable to connect to database: %s', dbString);
          console.log('Aborting');
          exit();
        } else if (database == 'index') {
          db.check_stats(settings.coin, function(exists) {
            if (exists == false) {
              console.log('Run \'npm start\' to create database structures before running this script.');
              exit();
            } else {
              db.update_db(settings.coin, function(){
                db.get_stats(settings.coin, function(stats){
                  if (settings.heavy == true) {
                    db.update_heavy(settings.coin, stats.count, 20, function(){
                    
                    });
                  }
                  if (mode == 'reindex') {
                    Tx.remove({}, function(err) { 
                      Address.remove({}, function(err2) { 
                        Richlist.update({coin: settings.coin}, {
                          received: [],
                          balance: [],
                        }, function(err3) { 
                          Stats.update({coin: settings.coin}, { 
                            last: 0,
                          }, function() {
                            console.log('index cleared (reindex)');
                          }); 
                          endidx = stats.count;
                          if (maxcnt !== -1){
                            if ((startidx + maxcnt - 1) <= endidx){
                              endidx = startidx + maxcnt - 1;
                            }
                          }else{
                            maxcnt = endidx - startidx + 1;
                          }
                          console.log("reindexing %s to %s (%s)", startidx, endidx, maxcnt);
                          db.update_tx_db(settings.coin, startidx, endidx, settings.update_timeout, function(){
                            db.update_richlist('received', function(){
                              db.update_richlist('balance', function(){
                                db.get_stats(settings.coin, function(nstats){
                                  console.log('reindex complete (block: %s, count: %s)', nstats.last, maxcnt);
                                  exit();
                                });
                              });
                            });
                          });
                        });
                      });
                    });              
                  } else if (mode == 'check') {
                    endidx = stats.count;
                    if (maxcnt !== -1){
                      if ((startidx + maxcnt - 1) <= endidx){
                        endidx = startidx + maxcnt - 1;
                      }
                    }else{
                      maxcnt = endidx - startidx + 1;
                    }
                    console.log("check %s to %s (%s)", startidx, endidx, maxcnt);
                    db.update_tx_db(settings.coin, startidx, endidx, settings.check_timeout, function(){
                      db.get_stats(settings.coin, function(nstats){
                        console.log('check complete (block: %s, count: %s)', nstats.last, maxcnt);
                        exit();
                      });
                    });
                  } else if (mode == 'update') {
                    startidx = stats.last + 1;
                    endidx = stats.count;
                    if (maxcnt !== -1){
                      if ((startidx + maxcnt - 1) <= endidx){
                        endidx = startidx + maxcnt - 1;
                      }
                    }else{
                      maxcnt = endidx - startidx + 1;
                    }
                    console.log("update %s to %s (%s)", startidx - 1, endidx, maxcnt);
                    db.update_tx_db(settings.coin, stats.last, endidx, settings.update_timeout, function(){
                      //db.update_richlist('received', function(){
                        //db.update_richlist('balance', function(){
                          db.get_stats(settings.coin, function(nstats){
                            console.log('update complete (block: %s, count: %s)', nstats.last, maxcnt);
                            exit();
                          });
                        //});
                      //});
                    });
                  } else if(mode == 'richlist'){
                    db.update_richlist('received', function(){
                      db.update_richlist('balance', function(){
                        console.log('richlist update complete');
                        exit();
                      });
                    });
                  }
                });
              });
            }
          });
        } else {
          //update markets
          var markets = settings.markets.enabled;
          var complete = 0;
          for (var x = 0; x < markets.length; x++) {
            var market = markets[x];
            db.check_market(market, function(mkt, exists) {
              if (exists) {
                db.update_markets_db(mkt, function(err) {
                  if (!err) {
                    console.log('%s market data updated successfully.', mkt);
                    complete++;
                    if (complete == markets.length) {
                      exit();
                    }
                  } else {
                    console.log('%s: %s', mkt, err);
                    complete++;
                    if (complete == markets.length) {
                      exit();
                    }
                  }
                });
              } else {
                console.log('error: entry for %s does not exists in markets db.', mkt);
                complete++;
                if (complete == markets.length) {
                  exit();
                }
              }
            });
          }
        }
      });
    });
  }
});
