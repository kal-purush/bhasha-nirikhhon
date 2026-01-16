var sqlite = require('sqlite3')
var fs = require('fs')
var crypto = require('crypto')
var async = require('async')

module.exports = {
  layers: [],

  databases: [],

  datasets: [],

  init: function(dir){
    var self = this
    fs.readdir(dir,function(err,files){
      if(err) throw err
      files.forEach(function(file){
        if(file.substr(-3) === '.db'){
          //Create our database index element
          var dbInfo = {}
          var fullPath = dir + '\\' + file
          //Create our db ID
          var fileNameCheckSum = crypto.createHash('md5')
          fileNameCheckSum.update(fullPath)
          var fileId = fileNameCheckSum.digest('hex')
          dbInfo.id = fileId
          dbInfo.name = file
          dbInfo.data = {}
          self.databases.push({fileId:fullPath})

          //Lets write some sql, queries in double quotes so that you can single quote strings inside
          var metaQuery = "SELECT * FROM metadata"
          var tableQuery = "SELECT * FROM sqlite_master WHERE type = 'table'"
          var layerQuery = "SELECT distinct time FROM " // will just add on the table name at query time

          var db = new sqlite.Database(fullPath, function(){

            //ok, now we can call our queries in the right order and populate our object
            async.series([

              function(callback){
                var metadata = {}
                db.all(metaQuery, function(err, rows){
                  if(err) return callback(err,null)

                  rows.forEach(function(row){
                    if(row.name.indexOf('project_name')!==-1){
                      dbInfo.project_name = row.value
                      var projectHash = crypto.createHash('md5')
                      projectHash.update(row.value)
                      dbInfo.project_hash = projectHash.digest('hex')
                    }
                    metadata[row.name] = row.value
                  })

                  //dbInfo[fileNameCheckSum].metadata = metadata
                  callback(null,metadata)
                })
              },

              function(callback){
                var layers = []
                db.all(tableQuery, function(err,rows){
                  if(err) return callback(err,null)

                    rows.forEach(function(table){
                      if(table.name !== 'metadata'){

                        var sql = layerQuery + table.name
                        db.all(sql, function(err,rows){
                          if(err) return callback(err,null)

                          rows.forEach(function(row){
                            var layerCheckSum = crypto.createHash('md5')
                            layerCheckSum.update(fileId + table.name + row.time)
                            var layerid = layerCheckSum.digest('hex')
                            //this gets sent to the client for listing datasets
                            layers.push({
                              layerid:layerid,
                              table:table.name,
                              time:row.time
                            })
                            //this stays in the server and helps us get the tiles, since we're storing the full path to the database
                            self.layers.push({
                              layerid:layerid,
                              db:fullPath,
                              table:table.name,
                              time:row.time
                            })
                          })

                        })
                      }
                    })
                    callback(null,layers)
                })
              }

            ],
            // basically a finally callback
            function(err, results){
              if(err){
                db.close()
                throw err
              }else{
                db.close()
                dbInfo.data.metadata = results[0]
                dbInfo.data.layers = results[1]
                self.datasets.push(dbInfo)
              }
            })

          })
        }
      })
    })
  },

  getImage: function(layerid, z, x, y, cb){
    var dset = this.layers.filter(function(layer){
      return layer.layerid === layerid
    })[0]
    if(y.indexOf('.png') !== -1) y = y.substring(0, y.length - 4)

    var sql = "SELECT * FROM " + dset.table + " where time = '" + dset.time +
        "' AND zoom_level = " + z +
        " AND tile_column = " + x +
        " AND tile_row = " + y

    var db = new sqlite.Database(dset.db, function(){
      var tileData
      db.get(sql, function(err, tile){
        if(typeof tile == 'undefined'){
          db.close()
          // fs.readFile('../blank.png', function (err, data) {
          //   if (err) throw err;
          //   return cb(data)
          // });
          tileData = null
        }else{
          db.close()
          tileData = tile.tile_data
        }
        return cb(tileData)
      })
    })
  }

}