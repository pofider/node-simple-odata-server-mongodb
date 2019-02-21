/*!
 * Copyright(c) 2014 Jan Blaha (pofider)
 *
 * Configure ODataServer to run on mongodb
 */

var ObjectId = require('mongodb').ObjectID

var cfg = null
var locale = null
var hexTest = /^[0-9A-Fa-f]{24}$/

function _convertStringsToObjectIds (o) {
  for (var i in o) {
    if ((i === '_id' || i.indexOf('Id') > -1 || i.indexOf('$') === 0) && (typeof o[i] === 'string' || o[i] instanceof String) && hexTest.test(o[i])) {
      o[i] = new ObjectId(o[i])
    }

    if ((i.indexOf('Ids') > -1) && (o[i] instanceof Array)) {
      for (var x = 0; x < o[i].length; x++) {
        if ((typeof o[i][x] === 'string' || o[i][x] instanceof String) && hexTest.test(o[i][x])) {
          o[i][x] = new ObjectId(o[i][x])
        }
      }
    }

    if (o[i] !== null && typeof (o[i]) === 'object') {
      _convertStringsToObjectIds(o[i])
    }
  }
};

function _handleFilter (o) {
  Object.keys(o).forEach((key) => {
    if (key.indexOf('/') > 0) {
      o[key.split('/').join('.')] = o[key]
      delete o[key]
    }

    if (typeof o[key] === 'object') {
      _handleFilter(o[key])
    }
  })
};

function update (getDB) {
  return function (collection, query, update, req, cb) {
    _convertStringsToObjectIds(query)
    _convertStringsToObjectIds(update)

    if (update.$set) {
      delete update.$set._id
    }

    getDB(function (err, db) {
      if (err) {
        return cb(err)
      }

      if (req.res.locals.db != null) {
        db = req.res.locals.db
      }

      db.collection(collection).updateOne(query, update, function (err, res) {
        if (err) {
          return cb(err)
        }

        if (res.matchedCount !== 1) {
          return cb(new Error('Update not successful'))
        }
        return cb(null, res.matchedCount)
      })
    })
  }
}

function remove (getDB) {
  return function (collection, query, req, cb) {
    _convertStringsToObjectIds(query)

    getDB(function (err, db) {
      if (err) {
        return cb(err)
      }

      if (req.res.locals.db != null) {
        db = req.res.locals.db
      }

      db.collection(collection).remove(query, cb)
    })
  }
}

function insert (getDB) {
  return function (collection, doc, req, cb) {
    _convertStringsToObjectIds(doc)

    getDB(function (err, db) {
      if (err) {
        return cb(err)
      }

      if (req.res.locals.db != null) {
        db = req.res.locals.db
      }

      db.collection(collection).insert(doc, function (err, res) {
        if (err) {
          return cb(err)
        }

        if (res.ops.length !== 1) {
          return cb(new Error('Mongo insert should return single document'))
        }

        cb(null, res.ops[0])
      })
    })
  }
}

function query (getDB) {
  return function (collection, query, req, cb) {
    _convertStringsToObjectIds(query)
    _handleFilter(query.$filter)

    getDB(function (err, db) {
      if (err) {
        return cb(err)
      }

      if (req.res.locals.db != null) {
        db = req.res.locals.db
      }

      var qr = null
      var aggregate = []

      if (query.$expand) {
        var joins = cfg.model.entitySets[collection].joins

        query.$expand.forEach((expand) => {
          aggregate.push({ $lookup: joins[expand] })
        })

        if (Object.keys(query.$filter).length > 0) {
          aggregate.push({ $match: query.$filter })
        }

        if (Object.keys(query.$select).length > 0) {
          aggregate.push({ $project: query.$select })
        }

        qr = db.collection(collection).aggregate(aggregate)
      } else {
        qr = db.collection(collection).find(query.$filter, { projection: query.$select || {} })
      }

      if (query.$sort) {
        qr = qr.collation({ locale: locale })
        qr = qr.sort(query.$sort)
      }
      if (query.$skip) {
        qr = qr.skip(query.$skip)
      }
      if (query.$limit) {
        qr = qr.limit(query.$limit)
      }

      if (query.$count) {
        return qr.count(cb)
      }

      if (!query.$inlinecount) {
        return qr.toArray(cb)
      }

      qr.toArray(function (err, res) {
        if (err) {
          return cb(err)
        }

        if (aggregate.length > 0) {
          db.collection(collection).aggregate(aggregate).toArray(function (err, c) {
            if (err) {
              return cb(err)
            }

            cb(null, {
              count: c.length,
              value: res
            })
          })
        } else {
          db.collection(collection).find(query.$filter).count(function (err, c) {
            if (err) {
              return cb(err)
            }

            cb(null, {
              count: c,
              value: res
            })
          })
        }
      })
    })
  }
}

module.exports = function (getDB) {
  return function (odataServer) {
    cfg = odataServer.cfg
    locale = cfg.locale ? cfg.locale : 'en'
    odataServer.update(update(getDB))
      .remove(remove(getDB))
      .query(query(getDB))
      .insert(insert(getDB))
  }
}
