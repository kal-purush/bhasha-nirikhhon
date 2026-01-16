const Tracker = require("../models/tracker.js");

module.exports = {
    /*
    POST: create a new tracker
    req.body = {
        name: String
        project: String
        description: String
        start: Date
    }
    */
    create: function(req, res){
        let tracker = new Tracker({
            name: req.body.name,
            project: req.body.project,
            description: req.body.description,
            start: new Date(req.body.start),
            pauses: []
        });

        tracker.save()
            .then((tracker)=>{
                return res.json(tracker);
            })
            .catch((err)=>{
                return res.json(err);
            });
    }
}