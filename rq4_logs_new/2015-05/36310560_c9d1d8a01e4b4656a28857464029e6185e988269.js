var React = require('react');
var Pouch = require('pouchdb');
var Browser = require('../Browser.jsx');

var db;
db = new Pouch('test-db');
init();
// require('little-pouch-db')(new Pouch('test-db'))
//   .then(function(pouch) {
//     window.db = db = pouch;
//     init();
//   })
// ;

var actionCreator = {

};

function init() {

  /*
  Fields are like:
  [
    {
      id: ...
      name: ...
      values: [
        { ... },
        { ... }
      ]
    }
  ]
   */

  var fields = {};
  db.allDocs({startkey:'field', endkey:'field~', include_docs: true})
    .then(function(resp) {
      resp.rows.forEach(function(row) {
        fields[row.doc._id] = row.doc;
        fields[row.doc._id].values = [];
      });
    })
    .then(function() {
      return db.query('valuesForField/valuesForField', {include_docs: true});
    })
    .then(function(resp) {
      resp.rows.forEach(function(row) {
        fields[row.key].values.push(row.doc);
      });
      var fieldList = Object.keys(fields).map(function(fieldKey) {
        return fields[fieldKey];
      });
      fields = fieldList;
      render(fields);
    })
    .catch(console.log.bind(console))
  ;

  function render(fields) {
    React.render(<Browser fields={fields} />, document.body);
  }
}