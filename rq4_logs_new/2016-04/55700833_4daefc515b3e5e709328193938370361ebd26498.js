var API_PATH = "http://data.taipei/opendata/datalist/apiAccess?scope=datasetMetadataSearch&q=id:ac589468-529b-4636-a9b2-ab57ae41cbcb";

function getData(cb) {
  $.ajax({
    url : API_PATH,
    method : "GET",
    success : function(data) {
      cb(null, data);
    },
    error : function(err) {
      cb(err);
    }
  });
}