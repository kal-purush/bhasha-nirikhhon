const express = require('express');
const { client } = require('../../database');
const wishlistRoute = express.Router();
const { v4: uuidv4 } = require('uuid');

wishlistRoute.post("/wishlist/add", (req, res) => {
    const { image ,productname ,storeid ,customerid ,productid ,price  }=req.body;
    const text = `
        INSERT INTO 
        wishlist(id,image ,productname ,storeid ,customerid ,productid ,price) 
        VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *
    `;
    client.query(text,[uuidv4(),image ,productname ,storeid ,customerid ,productid ,price],(err, data) => {
        if (err) {
            console.log(err)
            res.send({data:err});
        } else {
            res.send({data:data.rows[0]})
        }
    })
})
wishlistRoute.post("/wishlist/get/id", (req, res) => {
    const { id } = req.body;
    client.query(`SELECT * FROM wishlist where customerid='${id}'`, (err, data) => {
        if (err) {
            res.status(401).send({ data: err });
        } else {
            res.status(200).send({ data: data.rows })
        }
    })
})

wishlistRoute.delete("/wishlist/delete", (req, res) => {
    const { id } = req.body;
    client.query(`delete from wishlist where id='${id}'`, (err, data) => {
        if (err) {
            console.log(err);
            res.status(401).send({ data: err });
        } else {
            res.status(200).send({ data: 'Delete Item' })
        }
    })
})
wishlistRoute.delete("/wishlist/delete/productcustomer", (req, res) => {
    const { productid,customerid } = req.body;
    client.query(`delete from wishlist where customerid='${customerid}'and productid='${productid}'`, (err, data) => {
        if (err) {
            console.log(err);
            res.status(401).send({ data: err });
        } else {
            res.status(200).send({ data: 'Delete Item' })
        }
    })
})

module.exports = wishlistRoute;