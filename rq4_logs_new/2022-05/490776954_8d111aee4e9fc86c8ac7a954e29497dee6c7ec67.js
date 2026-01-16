const express = require("express");
const multer = require("multer");
const path = require("path");

const app = express();

const MIME_TYPE = {
	"image/png": "png",
	"image/jpg": "jpg",
	"image/jpeg": "jpeg",
};

const storage = multer.diskStorage({
	destination: function (req, file, cb) {
		cb(null, "./images");
	},
	filename: function (req, file, cb) {
		cb(
			null,
			file.fieldname + "-" + Date.now() + "." + MIME_TYPE[file.mimetype]
		);
	},
});

const upload = multer({
	storage,
	limits: {
		fileSize: 2000000,
	},
	fileFilter: (req, file, cb) => {
		const uploadFile = !!MIME_TYPE[file.mimetype];
		if (!uploadFile) {
			return cb(new Error("Error with wrong file"));
		}
		cb(null, true);
	},
}).single("profile");

app.post("/upload", (req, res, next) => {
	console.log("upload was called");

	upload(req, res, (err) => {
		if (err instanceof multer.MulterError) {
			res.status(500).json({ message: err.message });
		} else if (err) {
			res.status(500).json({ message: err.message });
		} else {
			console.log(req.file);
			res.json({ message: "upload successful!" });
		}
	});
});

// catch all unregistered route
app.use((req, res) => {
	res.status(400).json({ message: "page not found" });
});

// error middleware
app.use((error, req, res) => {
	res.status(500).json({ message: error.message });
});

app.listen(5000, () => console.log("server currently running on port 5000"));