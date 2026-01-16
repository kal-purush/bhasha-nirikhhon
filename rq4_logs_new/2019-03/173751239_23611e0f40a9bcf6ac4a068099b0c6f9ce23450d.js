const Client = require ('../models/client.model.js');
const fs = require('fs');

exports.createClient = (req, res) => {
    let client =  new Client ({
        NomSociete: req.body.NomSociete,
        Adresse: {
            Rue : req.body.Adresse.Rue,
            Ville : req.body.Adresse.Ville,
            CP : req.body.Adresse.CP,
        },
        ContactReference: {
            nom : req.body.ContactReference.nom,
            prenom : req.body.ContactReference.prenom,
            Telephone : req.body.ContactReference.Telephone,
            mail: req.body.ContactReference.mail
        },
        Secteur: req.body.Secteur
    });
    client.save((err) => {
        if (err) {
            console.log(err);
        }
        else {
            console.log('Client ajoute ');
        }
        res.send(client);
    })

}

exports.getClient = (req, res) => {
    Client.find((err, client) => {
        if (err){
            console.log(err);
        }
        res.send(client);
    })
}

exports.getClientById = (req, res) => {
    Client.findById(req.params.id, (err, client) => {
        if (err){
            console.log(err);
        }
        res.send(client);
    })
}

exports.UpdateClient = (req, res) => {
    Client.findByIdAndUpdate(req.params.id, req.body, (err, client) => {
        if (err){
            console.log(err);
        }
        else{
            console.log("Client a jour");
        }
        res.send(client);

    })
}

exports.removeClient = (req, res) => {
    Client.findByIdAndRemove(req.params.id, req.body, (err, client) => {
        if (err){
            console.log(err);
        }
        else{
            console.log("Client supprime");
        }
        res.send(client);

    })
}