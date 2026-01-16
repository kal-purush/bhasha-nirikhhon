var http = require("http");

//menentukan property options yang akan digunakan
var options = {
  hostname: "www.google.com",
  port: 80,
  path: "/",
  method: "GET",

  //header digunakan untuk menentukan tipe header yang digunakan seperti tipe konten dll.
  headers: {
    "Content-Type": "application/json",
  },
};

var req = http.request(options, function (response) {
  console.log(response.statusCode);
  console.log(response.statusMessage);
  console.log(response.headers);
});

//untuk handle error
req.on("error", function (e) {
  console.log("Error: " + e.message);
});

//tutup koneksi diakhir.
req.end();

//Jalankan dengan node httpClient.js