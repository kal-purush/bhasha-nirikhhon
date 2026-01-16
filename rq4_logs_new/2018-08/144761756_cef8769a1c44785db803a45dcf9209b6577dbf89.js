const express = require("express");
const app = express();
let port = 8000;

app.get("/", (req, res) => {
	res.send("Up and runnning");
})

app.listen(port, () => {
	console.log("Test listening to port: ", port);
});