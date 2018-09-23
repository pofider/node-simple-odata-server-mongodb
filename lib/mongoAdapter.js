/*!
 * Copyright(c) 2014 Jan Blaha (pofider)
 *
 * Configure ODataServer to run on mongodb
 */

var ObjectId = require('mongodb').ObjectID

var cfg = null
var locale = null;
var hexTest = /^[0-9A-Fa-f]{24}$/

function _convertStringsToObjectIds (o) {
  for (var i in o) {
    if ((i === '_id' || i.indexOf('Id') > -1) && (typeof o[i] === 'string' || o[i] instanceof String) && hexTest.test(o[i])) {
      o[i] = new ObjectId(o[i])
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
  });
};

function update (getDB) {
  return function (collection, query, update, req, cb) {
    _convertStringsToObjectIds(query)

    if (update.$set) {
      delete update.$set._id
    }

    getDB(function (err, db) {
      if (err) {
        return cb(err)
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

      db.collection(collection).remove(query, cb)
    })
  }
}

function insert (getDB) {
  return function (collection, doc, req, cb) {
    getDB(function (err, db) {
      if (err) {
        return cb(err)
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

      var qr = null

      if (query.$expand) {
        var joins = cfg.model.entitySets[collection].joins
        var aggregate = []

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

        db.collection(collection).find(query.$filter).count(function (err, c) {
          if (err) {
            return cb(err)
          }

          cb(null, {
            count: c,
            value: res
          })
        })
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
