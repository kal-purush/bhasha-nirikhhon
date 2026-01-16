'use strict'
const r = require('rethinkdb')
let conn = r.connect({
  host: 'localhost',
  port: 28015,
})
.then(() => {
  console.log('connected to database')
})
.catch((e) => {
  console.error(e.message)
})
// var xlsx = require('async-xlsx');
// var fs = require('fs')
// var data = [[1,2,3],[true, false, null, 'sheetjs'],['foo','bar',new Date('2014-02-19T14:30Z'), '0.3']];
// xlsx.buildAsync( [{name: "SheetName", data:data }], {}, function(error, xlsBuffer) {
//     if(!error){
//     fs.writeFileSync('campaña.xlsx',xlsBuffer,'binary', function(err) {
//       if (err) console.error(err.message)
//       console.log('archivo creado')
//     })
//         // Buffer is ready.
//     }
// });