const SleepsModel = require('../models/sleepsModel');

async function getSleeps(req,res) {
    try {
        const sleeps = await SleepsModel.find();
        res.status(200).json(sleeps);
    } catch (error) {
        res.status(500).json({ error: "Couldn't get sleeps!"});
    }
}

async function addSleep(req, res) {
    try {
        const start_time = req.body.start_time || new Date().toISOString();
        const sleep = await SleepsModel.insert({...req.body, start_time});
        res.status(201).json(sleep);
    } catch (error) {
        res.status(500).json({error: "Coudln't add sleep!"});
    }
}

async function updateSleep(req, res) {
    try {
        if(!req.params.id) {
            res.status(400).json({ error: "Please provide an ID" });
        }
        const id = req.params.id;
        const newSleep = await SleepsModel.update(req.body, id);
        res.status(200).json(newSleep);
    } catch (error) {
        res.status(500).json({ error: "Couldn't update sleep"});
    }
}

async function deleteSleep(req, res) {
    try {
        if(!req.params.id) {
            res.status(400).json({ error: "Please provide an ID" });
        }
        const id = req.params.id;
        const oldSleep = await SleepsModel.remove(id);
        res.status(200).json(oldSleep);
    } catch (error) {
        res.status(500).json({ error: "Couldn't remove sleep"});
    }
}

module.exports = {
    getSleeps,
    addSleep,
    updateSleep,
    deleteSleep
};