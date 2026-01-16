module.exports = get

var rdf = require('rdf-ext')

function get (webid, contentType, callback) {
  if (typeof contentType === 'function') {
    contentType = 'application/ld+json'
    callback = contentType
  }
  var store = new rdf.LdpStore()
  var IRI = webid.split('#')[0]

  store.graph(IRI, function (graph, err) {
    if (err) return callback(err)

    var triples = graph.match(webid, null, null)
    var obj = rdf.utils.mimeTypeSerializerMap[contentType](triples)
    callback(err, obj[0])
  })
}