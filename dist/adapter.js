'use strict';

Object.defineProperty(exports, '__esModule', {
  value: true
});

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { 'default': obj }; }

var _oracledb = require('oracledb');
//_oracledb.fetchAsString = [ _oracledb.NUMBER ]; 

var _oracledb2 = _interopRequireDefault(_oracledb);

var _utils = require('./utils');

var _utils2 = _interopRequireDefault(_utils);

var _async = require('async');

var _async2 = _interopRequireDefault(_async);

var _waterlineSequel = require('waterline-sequel');

var _waterlineSequel2 = _interopRequireDefault(_waterlineSequel);

var _lodash = require('lodash');

var _lodash2 = _interopRequireDefault(_lodash);

var createcount = 0;

_oracledb2['default'].outFormat = _oracledb2['default'].OBJECT;

var adapter = {

  connections: new Map(),

  syncable: true,
  schema: true,

  sqlOptions: {
    parameterized: true,
    caseSensitive: false,
    escapeCharacter: '"',
    casting: false,
    canReturnValues: false,
    escapeInserts: true,
    declareDeleteAlias: false,
    explicitTableAs: false,
    prefixAlias: 'alias__',
    stringDelimiter: "'",
    rownum: true,
    paramCharacter: ':',
    convertDate: false
  },

  /**
   * This method runs when a connection is initially registered
   * at server-start-time. This is the only required method.
   *
   * @param  {[type]}   connection [description]
   * @param  {[type]}   collection [description]
   * @param  {Function} cb         [description]
   * @return {[type]}              [description]
   */
  registerConnection: function registerConnection(connection, collections, cb) {
    var _this = this;

    if (!connection.identity) return cb(new Error('no identity'));
    if (this.connections.has(connection.identity)) return cb(new Error('too many connections'));

    var cxn = {
      identity: connection.identity,
      collections: collections || {},
      schema: adapter.buildSchema(connection, collections)
    };

    // set up some default values based on defaults for node-oracledb client
    var poolMin = connection.poolMin >= 0 ? connection.poolMin : 1;
    var poolMax = connection.poolMax > 0 ? connection.poolMax : 4;
    var poolIncrement = connection.poolIncrement > 0 ? connection.poolIncrement : 1;
    var poolTimeout = connection.poolTimeout >= 0 ? connection.poolTimeout : 1;
    var stmtCacheSize = connection.stmtCacheSize >= 0 ? connection.stmtCacheSize : 30;
    var prefetchRows = connection.prefetchRows >= 0 ? connection.prefetchRows : 100;
    var enableStats = connection.enableStats ? true : false;

    if (connection.maxRows > 0) {
      _oracledb2['default'].maxRows = connection.maxRows;
    }
    if (connection.queueTimeout >= 0) {
      _oracledb2['default'].queueTimeout = connection.queueTimeout;
    }

    var poolconfig = {
      _enableStats: enableStats,
      user: connection.user,
      password: connection.password,
      connectString: connection.connectString,
      poolMin: poolMin,
      poolMax: poolMax,
      poolIncrement: poolIncrement,
      poolTimeout: poolTimeout,
      stmtCacheSize: stmtCacheSize
    };

    // set up connection pool
    _oracledb2['default'].createPool(poolconfig, function (err, pool) {
      if (err) return cb(err);
      cxn.pool = pool;
      _this.connections.set(cxn.identity, cxn);
      cb();
    });
  },

  /**
   * Construct the waterline schema for the given connection.
   *
   * @param connection
   * @param collections[]
   */
  buildSchema: function buildSchema(connection, collections) {
    return _lodash2['default'].chain(collections).map(function (model, modelName) {
      var definition = _lodash2['default'].get(model, ['waterline', 'schema', model.identity]);
      return _lodash2['default'].defaults(definition, {
        attributes: {},
        tableName: modelName
      });
    }).keyBy('tableName').value();
  },

  /**
   * Create a new table
   *
   * @param connectionName
   * @param collectionName
   * @param definition - the waterline schema definition for this model
   * @param cb
   */
  define: function define(connectionName, collectionName, definition, cb) {
    var cxn = this.connections.get(connectionName);
    var collection = cxn.collections[collectionName];
    if (!collection) return cb(new Error('No collection with name ' + collectionName));

    var queries = [];
    var query = {
      sql: 'CREATE TABLE "' + collectionName + '" (' + _utils2['default'].buildSchema(definition) + ')'
    };
    queries.push(query);

    // Handle auto-increment
    var autoIncrementFields = _utils2['default'].getAutoIncrementFields(definition);

    if (autoIncrementFields.length > 0) {
      // Create sequence and trigger queries for each one
      autoIncrementFields.forEach(function (field) {
        var sequenceName = _utils2['default'].getSequenceName(collectionName, field);
        queries.push({
          sql: 'CREATE SEQUENCE ' + sequenceName
        });
        var triggerSql = 'CREATE OR REPLACE TRIGGER ' + collectionName + '_' + field + '_trg\n                         BEFORE INSERT ON "' + collectionName + '"\n                         FOR EACH ROW\n                         BEGIN\n                         SELECT ' + sequenceName + '.NEXTVAL\n                         INTO :new."' + field + '" FROM dual;\n                         END;';

        queries.push({
          sql: triggerSql
        });
      });
    }

    // need to create sequence and trigger for auto increment
    return this.executeQuery(connectionName, queries, cb);
  },

  describe: function describe(connectionName, collectionName, cb) {
    var connectionObject = this.connections.get(connectionName);
    var collection = connectionObject.collections[collectionName];

    // have to search for triggers/sequences?
    var queries = [];
    queries.push({
      sql: 'SELECT COLUMN_NAME, DATA_TYPE, NULLABLE FROM USER_TAB_COLUMNS WHERE TABLE_NAME = \'' + collectionName + '\''
    });
    queries.push({
      sql: 'SELECT index_name,COLUMN_NAME FROM user_ind_columns WHERE TABLE_NAME = \'' + collectionName + '\''
    });
    queries.push({
      sql: 'SELECT cols.table_name, cols.column_name, cols.position, cons.status, cons.owner\n        FROM all_constraints cons, all_cons_columns cols WHERE cols.table_name =\n        \'' + collectionName + '\' AND cons.constraint_type = \'P\' AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner\n        ORDER BY cols.table_name, cols.position'
    });

    this.executeQuery(connectionName, queries, function (err, results) {
      var schema = results[0];
      var indices = results[1];
      var tablePrimaryKeys = results[2];
      var normalized = _utils2['default'].normalizeSchema(schema, collection.definition);
      if (_lodash2['default'].isEmpty(normalized)) {
        return cb();
      }
      cb(null, normalized);
    });
  },

  executeQuery: function executeQuery(connectionName, queries, cb) {
    var _this2 = this;

    if (!_lodash2['default'].isArray(queries)) {
      queries = [queries];
    }
    var cxn = null;
    if(this.connections[connectionName]){
      cxn = this.connections[connectionName]["_adapter"].connections.get(connectionName)
    }else{
      cxn = this.connections.get(connectionName);
    }
    
    if (cxn.pool._enableStats) {
      console.log(cxn.pool._logStats());
    }
    cxn.pool.getConnection(function (err, conn) {

      if (err && err.message.indexOf('ORA-24418') > -1) {
        // In this scenario, just keep trying until one of the connections frees up
        return setTimeout(_this2.executeQuery(connectionName, queries, cb).bind(_this2), 50);
      }

      if (err) return cb(err);

      _async2['default'].reduce(queries, [], function (memo, query, asyncCallback) {
        var options = {};

        // Autocommit by default
        if (query.autoCommit !== false) {
          options.autoCommit = true;
        }

        if (query.outFormat !== undefined) {
          options.outFormat = query.outFormat;
        }

        //console.log('executing', query.sql, query.params)
        conn.execute(query.sql, query.params || [], options, function (queryError, res) {
          if (queryError) return asyncCallback(queryError);
          memo.push(res);
          asyncCallback(null, memo);
        });
      }, function (asyncErr, result) {
        conn.release(function (error) {
          if (error) console.log('Problem releasing connection', error);
          if (asyncErr) {
            cb(asyncErr);
          } else {
            cb(null, _this2.handleResults(result));
          }
        });
      });
    });
  },

  handleResults: function handleResults(results) {
    return results.length == 1 ? results[0] : results;
  },

  teardown: function teardown(conn, cb) {
    var _this3 = this;

    if (typeof conn == 'function') {
      cb = conn;
      conn = null;
    }
    if (conn === null) {
      this.connections = {};
      return cb();
    }
    if (!this.connections.has(conn)) return cb();

    var cxn = this.connections.get(conn);
    cxn.pool.close().then(function () {
      _this3.connections['delete'](conn);
      cb();
    })['catch'](cb);
  },

  createEach: function createEach(connectionName, table, records, cb) {
    cb();
  },



  // Add a new row to the table
  create: function create(connectionName, table, data, cb) {
    var connectionObject = this.connections.get(connectionName);
    var collection = connectionObject.collections[table];

    var schemaName = collection.meta && collection.meta.schemaName ? _utils2['default'].escapeName(collection.meta.schemaName) + '.' : '';
    var tableName = schemaName + _utils2['default'].escapeName(table);

    // Build up a SQL Query
    var schema = connectionObject.schema;
    //var processor = new Processor(schema);

    // Prepare values
    Object.keys(data).forEach(function (value) {
      data[value] = _utils2['default'].prepareValue(data[value]);
    });

    var definition = collection.definition;
    _lodash2['default'].each(definition, function (column, name) {

      if (fieldIsBoolean(column)) {
        // no boolean type in oracle, so save it as a number
        data[name] = data[name] ? 1 : 0;
      }
    });

    var sequel = new _waterlineSequel2['default'](schema, this.sqlOptions);

    var incrementSequences = [];
    var query = undefined;

    try {
      query = sequel.create(table, data);
    } catch (e) {
      return cb(e);
    }

    var returningData = _utils2['default'].getReturningData(collection.definition);

    var queryObj = {};

    if (returningData.params.length > 0) {
      query.query += ' RETURNING ' + returningData.fields.join(', ') + ' INTO ' + returningData.outfields.join(', ');
      query.values = query.values.concat(returningData.params);
      queryObj.outFormat = _oracledb2['default'].OBJECT;
    }

    queryObj.sql = query.query;
    queryObj.params = query.values;

    this.executeQuery(connectionName, queryObj, function (err, results) {
      if (err) return cb(err);
      cb(null, _utils2['default'].transformBulkOutbinds(results.outBinds, returningData.fields)[0]);
    });
  },


  query: function query(connectionName, collectionName, query, data, cb, connection) {

    if (_.isFunction(data)) {
      cb = data;
      data = null;
    }

   
    if(data != null && _.isArray(data) && data.length > 0){
      data = adapter.formatParam(data);
      let sqls = query.split('?');
      if(sqls && sqls.length-1 ===data.length){
        query ='';
        for(let i=0;i<sqls.length;i++){

          query += sqls[i]+data[i];
        }
      }
    }
    console.debug("Query查询语句："+query);
    //query =  adapter.formatSql(query);
    adapter.executeQuery(connectionName, {
      sql: query,
      params: null//query.values[0]
    }, function (err, results) {
      if (err) return cb(err);
      cb(null, results && results.rows);
    });
  },

  formatParam:function(data){

    if(data != null && _.isArray(data) && data.length > 0){
      for(let i =0;i<data.length;i++){
        let value = data[i];
        if(_.isString(value)){
          let timeReg = /[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}/g;
          //console.log(newSql)
          let macths = value.match(timeReg);
          console.log(JSON.stringify(macths))
          if(macths !=null && macths.length >0){
              _.forEach(macths,function(m){
                if(value.indexOf("'")<0){
                  value = value.replace(m,"'"+m+"'");
                }
              
              });
            
          }
          else{
            let timeReg = /[0-9]{4}-[0-9]{2}-[0-9]{2}/g;
            let macths = value.match(timeReg);
            console.log(JSON.stringify(macths))
            if(macths !=null && macths.length >0){
                _.forEach(macths,function(m){
                  if(value.indexOf("'")<0){
                    value = value.replace(m,"'"+m+"'");
                  }
                });
              
            }
          }
          data[i] = value;
        }
      }

      
    }

    return data;
  },

  formatSql: function(sql){

    var _ = require('lodash');

//var sql ="select count(ani_num) count,DATE_FORMAT(create_date,'%Y-%m-%d') createDate from activity_news_info where create_date>='2018-05-16 00:00:00' and create_date<'2018-05-22 23:59:59' group by createDate";

//转小写

          sql = sql.toLowerCase();
          sql = sql.replace(/date_format/g,"dateformat")
          var sqls = sql.split(" ");

          var keywords=['select','count','from','where','group','by','desc','and'];

          var newSql ='';
          _.forEach(sqls,function(item){

              if(_.indexOf(keywords,item)>-1){
                  newSql += item;
              }else{
                  if(item.indexOf('_')>-1){
                      let items = item.split('_');
                      let itemStr ='';
                      for(let i=0;i<items.length;i++){
                          let subItem = items[i];
                        //  console.log("subItem:"+subItem)
                          let newSubItem ='';
                          let index = subItem.lastIndexOf('(');
                          if(index>-1){
                                newSubItem =subItem.substring(0,index+1)+'"'+subItem.substring(index+1)
                          }
                          else if((index = subItem.lastIndexOf('.'))>-1){
                            
                                  newSubItem =subItem.substring(0,index+1)+'"'+subItem.substring(index+1)
                              }
                          else if((index = subItem.indexOf(','))>-1){
                                if(i==0){
                                  newSubItem =subItem.substring(0,index)+',"'+subItem.substring(index+1)
                                }else{
                                  newSubItem =subItem.substring(0,index)+'",'+subItem.substring(index+1)
                                }
                                      }
                          else if((index = subItem.indexOf('>'))>-1){
                                                  newSubItem =subItem.substring(0,index)+'">'+subItem.substring(index+1)
                                              }
                          else if((index = subItem.indexOf('<'))>-1){
                                                          newSubItem =subItem.substring(0,index)+'"<'+subItem.substring(index+1)
                                                      }
                          else if((index = subItem.indexOf('='))>-1){
                                                                  newSubItem =subItem.substring(0,index)+'"='+subItem.substring(index+1)
                                                              }                           
                          else if((index = subItem.indexOf(')'))>-1){
                                      newSubItem =subItem.substring(0,index)+'")'+subItem.substring(index+1)
                                  }
                          else if(i==0){
                              newSubItem = '"'+subItem
                            }
                          else if(i==items.length-1){
                              newSubItem = subItem+'"';
                            }
                          else{
                                
                              newSubItem = subItem;
                            }  
                          
                          itemStr +=newSubItem+"_"
                      };
                      
                      itemStr = itemStr.substring(0,itemStr.length-1);
                      //console.log("itemStr:"+itemStr)
                      newSql += itemStr;
                  }else{
                      newSql += item;
                  }
                
              }
              newSql  +=' ';

              
          });


          newSql = newSql.replace(/dateformat/g,"to_char");
          newSql = newSql.replace(/%y/g,"yyyy");
          newSql = newSql.replace(/%m/g,"mm")
          newSql = newSql.replace(/%d/g,"dd");

          let timeReg = /'[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}'/g;
          //console.log(newSql)
          let macths = newSql.match(timeReg);
          console.log(JSON.stringify(macths))
          let strs='' //去重复字符串
          if(macths !=null && macths.length >0){
              _.forEach(macths,function(m){
                  if(strs.indexOf(m)==-1){
                      newSql = newSql.replace(new RegExp(m,"gm"),"to_date("+m+",'yyyy/mm/dd hh24:mi:ss')");
                      strs+=m;
                  }
              });
             
          }
          else{
              let timeReg = /'[0-9]{4}-[0-9]{2}-[0-9]{2}'/g;
              let macths = newSql.match(timeReg);
              console.log(JSON.stringify(macths));
              let strs='' //去重复字符串
              if(macths !=null && macths.length >0){
                  _.forEach(macths,function(m){
                    if(strs.indexOf(m)==-1){
                      newSql = newSql.replace(new RegExp(m,"gm"),"to_date("+m+",'yyyy/mm/dd')");
                      strs+=m;
                    }
                   
                  });
                
              }
            }
          //分页
        let index = newSql.indexOf('limit');
        if(index >-1){
            let copySql = newSql.substring(0,index);
            let pageInfo  = newSql.substring(index);
            newSql = copySql.replace('select','SELECT ROWNUM AS LINE_NUMBER,');
            let pageInfos  = pageInfo.split(" ");
            let skip=0;
            let limit=0;
            _.forEach(pageInfos,function(item){
            
                if(item.indexOf(',')>-1){
                    let infos = item.split(',');
                    skip=parseInt(infos[0]);
                    limit =parseInt(infos[1]);
                }
            
            
            });
            if (limit && skip) {
                newSql = 'SELECT * FROM (' + newSql + ') WHERE LINE_NUMBER > ' + skip + ' and LINE_NUMBER <= ' + (skip + limit);
              } else if (limit) {
                newSql = 'SELECT * FROM (' + newSql + ') WHERE LINE_NUMBER <= ' + limit;
              } else if (skip) {
                newSql = 'SELECT * FROM (' + newSql + ') WHERE LINE_NUMBER > ' + skip;
              }
        }
          console.log('格式化后：'+newSql);

          return newSql;
  },

  find: function find(connectionName, collectionName, options, cb, connection) {
    var connectionObject = this.connections.get(connectionName);
    var collection = connectionObject.collections[collectionName];

    var sequel = new _waterlineSequel2['default'](connectionObject.schema, this.sqlOptions);
    var query = undefined;
    var limit = options.limit || null;
    var skip = options.skip || null;
    delete options.skip;
    delete options.limit;

    // Build a query for the specific query strategy
    try {
      query = sequel.find(collectionName, options);
    } catch (e) {
      return cb(e);
    }

    var findQuery = query.query[0];

    let querys = findQuery.split(',');
    let sql ="";
    for(let i = 0;i <querys.length;i++){
      if(sql.indexOf(querys[i]) <0){
        if(i!=0){
          sql+=',';
        }
        sql+=querys[i];
          }

    }

    findQuery = sql;
    if (limit && skip) {
      findQuery = 'SELECT * FROM (' + findQuery + ') WHERE LINE_NUMBER > ' + skip + ' and LINE_NUMBER <= ' + (skip + limit);
    } else if (limit) {
      findQuery = 'SELECT * FROM (' + findQuery + ') WHERE LINE_NUMBER <= ' + limit;
    } else if (skip) {
      findQuery = 'SELECT * FROM (' + findQuery + ') WHERE LINE_NUMBER > ' + skip;
    }
    console.debug('查询语句'+findQuery);
    this.executeQuery(connectionName, {
      sql: findQuery,
      params: query.values[0]
    }, function (err, results) {
      if (err) return cb(err);
      cb(null, results && results.rows);
    });
  },

  destroy: function destroy(connectionName, collectionName, options, cb, connection) {
    var _this4 = this;

    var connectionObject = this.connections.get(connectionName);
    var collection = connectionObject.collections[collectionName];

    var query = undefined;
    var sequel = new _waterlineSequel2['default'](connectionObject.schema, this.sqlOptions);

    try {
      query = sequel.destroy(collectionName, options);
    } catch (e) {
      return cb(e);
    }

    var handler = function handler(err, findResult) {
      if (err) return cb(err);
      _this4.executeQuery(connectionName, {
        sql: query.query,
        params: query.values
      }, function (delErr, delResult) {
        // TODO: verify delResult?
        if (delErr) return cb(delErr);
        cb(null, findResult);
      });
    };

    return this.find(connectionName, collectionName, options, handler, connection);
  },

  drop: function drop(connectionName, collectionName, relations, cb, connection) {
    var _this5 = this;

    if (typeof relations == 'function') {
      cb = relations;
      relations = [];
    }

    relations.push(collectionName);

    var queries = relations.reduce(function (memo, tableName) {
      memo.push({
        sql: 'DROP TABLE "' + tableName + '"'
      });
      var connectionObject = _this5.connections.get(connectionName);
      var collection = connectionObject.collections[tableName];

      var autoIncrementFields = _utils2['default'].getAutoIncrementFields(collection.definition);
      if (autoIncrementFields.length > 0) {
        autoIncrementFields.forEach(function (field) {
          var sequenceName = _utils2['default'].getSequenceName(tableName, field);
          memo.push({
            sql: 'DROP SEQUENCE ' + sequenceName
          });
        });
      }
      return memo;
    }, []);

    return this.executeQuery(connectionName, queries, cb);
  },

  update: function update(connectionName, collectionName, options, values, cb, connection) {
    //    var processor = new Processor();
    var connectionObject = this.connections.get(connectionName);
    var collection = connectionObject.collections[collectionName];

    // Build find query
    var sequel = new _waterlineSequel2['default'](connectionObject.schema, this.sqlOptions);

    var query = undefined;
    // Build query
    try {
      query = sequel.update(collectionName, options, values);
    } catch (e) {
      return cb(e);
    }

    var returningData = _utils2['default'].getReturningData(collection.definition);
    var queryObj = {};

    if (returningData.params.length > 0) {
      query.query += ' RETURNING ' + returningData.fields.join(', ') + ' INTO ' + returningData.outfields.join(', ');
      query.values = query.values.concat(returningData.params);
    }

    queryObj.sql = query.query;
    queryObj.params = query.values;

    // Run query
    return this.executeQuery(connectionName, queryObj, function (err, results) {
      if (err) return cb(err);
      cb(null, _utils2['default'].transformBulkOutbinds(results.outBinds, returningData.fields));
    });
  }
};

function fieldIsBoolean(column) {
  return !_lodash2['default'].isUndefined(column.type) && column.type === 'boolean';
}

exports['default'] = adapter;
module.exports = exports['default'];
