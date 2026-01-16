const express = require("express");
const app = express();
const ytdl = require("ytdl-core");
app.set("view engine", "ejs");
// OUR ROUTES WILL GO HERE
app.listen(3000, () => {
	console.log("Server is running on http://localhost:3000");
});