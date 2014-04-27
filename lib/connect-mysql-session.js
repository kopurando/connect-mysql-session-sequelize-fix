var Sequelize = require('sequelize');

module.exports = function (connect)
{
    function MySQLStore(options)
    {
        options = options || {};
        connect.session.Store.call(this, options);
        
        var self = this,
            database = options.database || null,
            user = options.user || null,
            password = options.password || null, 
            table_name = options.table || 'Session', 
            forceSync = options.forceSync || false,
            checkExpirationInterval = options.checkExpirationInterval || 1000*60*10, // default 10 minutes.
            defaultExpiration = options.defaultExpiration || 1000*60*60*24; // default 1 day.
        
        var sequelize;
        if(options.sequelize)
          sequelize = options.sequelize;
        else
          sequelize = new Sequelize(database, user, password, options);

        var memcached,
            memcached_prefix = 'movy_sessions_';
        if(options.memcached)
          memcached = options.memcached;
        
        var Session = sequelize.define(table_name, {
            sid: {type: Sequelize.STRING, primaryKey: true, allowNull: false},
            expires: Sequelize.INTEGER,
            json: Sequelize.TEXT
        });
        
        var initialized = false;
        
        function initialize(callback)
        {
            if (initialized) callback();
            else
            {
                sequelize.sync({force: forceSync})
                .success(function ()
                {
                    console.log('MySQL session store initialized.');
                    initialized = true;
                    callback();
                })
                .error(function (error)
                {
                    console.log('Failed to initialize MySQL session store:');
                    console.log(error);
                    callback(error);
                });
            }
        }
        
        // Check periodically to clear out expired sessions.
        setInterval(function ()
        {
            initialize(function (error)
            {
                if (error) return;
                Session.findAll({where: ['expires < ?', Math.round(Date.now() / 1000)]})
                .success(function (sessions)
                {
                    if (sessions.length > 0)
                    {
                        console.log('Destroying ' + sessions.length + ' expired sessions.');
                        for (var i in sessions)
                        {
                          sessions[i].destroy();
                        }
                    }
                })
                .error(function (error)
                {
                    console.log('Failed to fetch expired sessions:');
                    console.log(error);
                });
            });
        }, checkExpirationInterval);

        var get_session = function(sid, callback) {
          if(!memcached)
            return get_session_db(sid, callback);
          memcached.get(memcached_prefix + sid, function(error, record) {
            if(error || !record || !record.json)
              return get_session_db(sid, callback);
            callback(error, record);
          })
        };

        var get_session_db = function(sid, callback) {
          Session.find({where: {sid: sid}})
          .success(function (record) {
            callback(null, record);
          }).error(function(error) { callback(error, null); });
        };

        
        this.get = function (sid, fn)
        {
          initialize(function (error)
          {
            if (error) return fn(error, null);
            get_session(sid, function(error, record) {
              if(error) return fn(error, null);

              var session = record && (typeof record.json == 'object'? record.json : JSON.parse(record.json));
              fn(null, session);
            });
          });
        };
        
        this.set = function (sid, session, fn)
        {
          initialize(function (error)
          {
            if (error) return fn && fn(error);

            get_session(sid, function(error, record) {
              if(error) return fn && fn(error);
              if (!record) {
                // Set expiration to match the cookie or 1 year in the future if unspecified.
                var expires = session.cookie.expires ||
                              new Date(Date.now() + defaultExpiration);
                return Session.create({ sid: sid, json: JSON.stringify(session),
                                 expires: Math.round(expires.getTime() / 1000)})
                .success(function(record) {
                  memcached.set(memcached_prefix + sid, record, record.expires - Math.round(Date.now() / 1000));
                  fn && fn();
                });
              }
              else if(record.json == JSON.stringify(session) && 
                      (record.expires - Math.round(Date.now() / 1000)) * 2 > defaultExpiration / 1000)
                return fn && fn();

              record.json = JSON.stringify(session);
              // Set expiration to match the cookie or 1 year in the future if unspecified.
              var expires = session.cookie.expires ||
                            new Date(Date.now() + defaultExpiration);
              // Note: JS uses milliseconds, but we want integer seconds.
              record.expires = Math.round(expires.getTime() / 1000);
              
              if(memcached) {
                memcached.set(memcached_prefix + sid, record, record.expires - Math.round(Date.now() / 1000));
              }
              Session.update({ json: record.json, expires: record.expires }, { sid: sid})
              .success(function() {
                  fn && fn();
              })
              .error(function(error) {
                  fn && fn(error);
              });
            })
          });
        };
        
        this.destroy = function (sid, fn)
        {
          initialize(function (error) {
            if (error) return fn && fn(error);
            if(memcached)
              memcached.del(memcached_prefix + sid, function(error) { });
            Session.find({where: {sid: sid}})
            .success(function (record)
            {
                if (record)
                {
                    record.destroy()
                    .success(function ()
                    {
                        fn && fn();
                    })
                    .error(function (error)
                    {
                        console.log('Session ' + sid + ' could not be destroyed:');
                        console.log(error);
                        fn && fn(error);
                    });
                }
                else fn && fn();
            })
            .error(function (error)
            {
                fn && fn(error);
            });
          });
        };
        
        this.length = function (callback)
        {
            initialize(function (error)
            {
                if (error) return callback(null);
                Session.count()
                .success(callback)
                .error(function () { callback(null); });
            });
        };
        
        this.clear = function (callback)
        {
            sequelize.sync({force: true}, callback);
        };
    }
    
    MySQLStore.prototype.__proto__ = connect.session.Store.prototype;
    
    return MySQLStore;
};
