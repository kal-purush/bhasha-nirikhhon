const Song = require('../models/Songs');

const removeEmptyOrNull = (obj) => {
    Object.keys(obj).forEach(k => {
        if (obj[k] === undefined || obj[k] === null) delete obj[k]
    });
    return obj;
};

// list
const index = (req, res, next) => {
    console.log(Song)
    Song.find().then(response => {
        res.json({
            response
        })
    })
        .catch(error => {
            res.json({
                message: "An error Occured!"
            })
        })
}

//single
const show = (req, res, next) => {
    let songID = req.params.id;
    Song.findById(songID).then(response => {
        res.json({
            response
        })
    }).catch(error => {
        res.json({ message: "An error Occured !", error: error })
    })
}

//store
const store = (req, res, next) => {
    const track = "E:Personal-Projects/syndicate-backend/uploads/track" + req.file.filename;
    let song = new Song({
        name: req.body.name,
        artist: req.body.artist,
        album: req.body.album,
        track,
        description: req.body.description,
        lyrics: req.body.lyrics,
        category: req.body.category,
        image: req.body.image,
        banner: req.body.banner
    })
    song.save().then(response => {
        res.json({
            message: "Song Added Successfully!"
        })
    }).catch(error => {
        res.json({
            message: "An error Occured!"
        })
    })
}

//update
const update = (req, res, next) => {
    const track = req.files.track ? "E:Personal-Projects/syndicate-backend/uploads/audio/" + req.files.track[0].filename : null;
    const image = req.files.image ? "E:Personal-Projects/syndicate-backend/uploads/image/" + req.files.image[0].filename : null;
    const banner = req.files.banner ? "E:Personal-Projects/syndicate-backend/uploads/image/" + req.files.banner[0].filename : null;
    let songID = req.body.songID
    let updatedData = {
        name: req.body.name,
        artist: req.body.artist,
        album: req.body.album,
        track,
        description: req.body.description,
        lyrics: req.body.lyrics,
        category: req.body.category,
        image,
        banner
    }

    const updatedFinalData = removeEmptyOrNull(updatedData);

    Song.findByIdAndUpdate(songID, { $set: updatedFinalData }).then(() => {
        res.json({
            message: "Song has been updated sunccessfully!"
        })
    }).catch(error => {
        res.json({
            message: "An error Occured!"
        })
    })
}

// delete
const deleteSong = (req, res, next) => {
    let songID = req.params.id;
    Song.findByIdAndRemove(songID).then(() => {
        res.json({
            message: "Song has been deleted sunccessfully!"
        })
    }).catch(error => {
        res.json({
            message: "An error Occured!",
            error: error
        })
    })
}

module.exports = {
    index, show, store, update, deleteSong
}