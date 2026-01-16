const multer = require("multer");

//création d'un dictionnaire sous forme d'objet pour les extensions des images
const MIME_TYPES = {
    "image/jpg": "jpg",
    "image/jpeg": "jpg",
    "image/png": "png",
};

//la const storage est passée à multer comme config avec la logique d'enregistrement
const storage = multer.diskStorage({
    destination: (req, file, callback) => {
        callback(null, "images");
    },
    filename: (req, file, callback) => {
        const name = file.originalname.split("").join("_"); //élimine le pb des espaces acceptés par certains browser
        const extension = MIME_TYPES[file.mimetype];
        callback(null, name + Date.now() + "." + extension);
    },
});

module.exports = multer({ storage }).single("image");