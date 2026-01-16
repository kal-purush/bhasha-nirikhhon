var sys = require("sys");
var exec = require("child_process").exec;

exec(
  "sudo mkdir /var/www/autoRedshift && sudo cp redshift.js /var/www/autoRedshift/",
  function(error, stdout, stderr) {
    console.log("Executed");

    //readableStream.pipe(writableStream);
    sys.print("stdout: " + stdout);
    sys.print("stderr: " + stderr);
    if (error !== null) {
      console.log("exec error: " + error);
    }
  }
);