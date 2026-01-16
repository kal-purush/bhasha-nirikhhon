const express = require('express');
const router = express.Router();
const folderModel = require('../models/folderModel');

router.get('/createfolder', (req, res)=>{
    const data = req.body;
    const folder = new folderModel({
        user: "srishti",
        folders: [
            {
                name: "Ring Ceremony (Cameraman)",
                url: "13Yd2YksdSmbpX0s4ScUhPGIlvoILOs6_",
            },
            {
                name: "Wedding photographs (Others)",
                url: "1657IokRr1imotSQHSXc6t6Hrl0C2wFuK",
            },
            {
                name: "Sriyush : Lagan Chhuayi",
                url: "1kNAOSsxkxz5MNuH5SPxAxPH5-zhtesZ1",
            },
            {
                name: "Sriyush : Sangeet",
                url: "1-6vWn9JKJfKCW2ItdxjpEQlN7Vnz2MR0"
            },
            {
                name: "Sriyush : Tilak",
                url: '1-1Fr66tSFrGp5cukEAOS-5eMzMXjIjTZ'
            },
            {
                name: "Sriyush : Wedding",
                url: "1-GBdSN5rd4UfbF9ha-r6mA8OFokjPXpX"
            },
            {
                name: "Sriyush: Mehendi",
                url: "1jqMt3eDVauBNU6jXIhmv8cEmqYqXSo5x"
            },
            {
                name: "Sriyush :Haldi",
                url: "1-C8ECmWNcjozEIFYYftmkqX7iOeuUYal"
            },
            {
                name: "Traditional",
                url: "16xEtOt_HRcY7eqARd3Qvjn86Rly083b_"
            },
            {
                name: "Candid",
                url: "1PEIzxoLLl7_OTxj2q0x6oi4QS1R6SheV"
            },
        ]
    })
    folder.save().then((result)=>{
        res.send({msg: "Folder created successfully!"});
    }).catch((err)=>{
        res.send({msg: 'Some error occured!'});
    })
})
module.exports = router;