import * as functions from 'firebase-functions';
import * as admin from 'firebase-admin';
import * as express from 'express';
import * as bodyParser from "body-parser";
import * as firebaseHelper from 'firebase-functions-helper';

const swaggerUi = require('swagger-ui-express');
const swaggerDocument = require('../swagger.json');

const firebase = admin.initializeApp(functions.config().firebase);
const app = express();
const main = express();
const db = admin.firestore();
const settings = {/* your settings... */ timestampsInSnapshots: true };
db.settings(settings);

main.use('/api-docs/', swaggerUi.serve, swaggerUi.setup(swaggerDocument));
main.use('/store/v1', app);
main.use(bodyParser.json());
main.use(bodyParser.urlencoded({ extended: false }));
const ordersCollection = 'orders';

export const orderAPI = functions.https.onRequest(main);

// View all orders
app.get('/order', (req, res) => {
    return firebaseHelper.firestore
        .backup(db, ordersCollection)
        .then(data => res.status(200).json({ 'orders': data }))
})

// View a order
app.get('/order/:orderId', (req, res) => {
    firebaseHelper.firestore
        .getDocument(db, ordersCollection, req.params.orderId)
        .then(data => res.status(200).json(data));
    return true;
})
function getQueryRef(ref, queryRef) {
    if (queryRef) {
        return queryRef
    }
    return ref;

}
// View a order
app.get('/order/filter/search', (req, res) => {
    console.log('testing.....')
    console.log('query :' + JSON.stringify(req.query))

    const ref = db.collection(ordersCollection);
    let queryRef = null;

    if (req.query.storeId) {
        queryRef = getQueryRef(ref, queryRef).where("storeId", "==", req.query.storeId);
    }
    if (req.query.operatingRoom) {
        queryRef = getQueryRef(ref, queryRef).where("operatingRoom", "==", req.query.operatingRoom);
    }
    if (req.query.orderStatus) {
        queryRef = getQueryRef(ref, queryRef).where("orderStatus", "==", req.query.orderStatus);
    }
    if (req.query.orderType) {
        queryRef = getQueryRef(ref, queryRef).where("orderType", "==", req.query.orderType);
    }
    if (req.query.sku) {
        const skuType = `skus.${req.query.sku}.sku`
        console.log('skuType: ' + skuType)
        queryRef = getQueryRef(ref, queryRef).where(skuType, "==", req.query.sku);
    }
    if (req.query.limit) {
        queryRef = getQueryRef(ref, queryRef).limit(req.query.limit)
    }
    if (queryRef) {
        queryRef.get().then(querySnapshot => {
            const data = querySnapshot.docs.map(documentSnapshot => {
                return documentSnapshot.data();
            });
            console.log("Orders retrieved: " + JSON.stringify(data));
            res.json(data);

        }).catch(err => {
            console.error("Error while getting document: ", err);
        });
    } else {
        firebaseHelper.firestore
            .backup(db, ordersCollection)
            .then(data => res.status(200).json(data))
    }
    return true;
})

// Add new order
app.post('/order', (req, res) => {
    firebaseHelper.firestore
        .createNewDocument(db, ordersCollection, req.body).then(error => {
            console.log(error);
            res.send('order created succesfully !!!');
        });
    return true;
})

// Update new order
app.patch('/order/:orderId', (req, res) => {
    firebaseHelper.firestore
        .updateDocument(db, ordersCollection, req.params.orderId, req.body);
    res.send('Update a new order');
})
app.put('/order/:orderId', (req, res) => {
    db.collection(ordersCollection).doc(req.params.orderId).set(req.body)
        .then(function () {
            console.log("Document successfully written!");
            res.send('Update a new order');
        })
        .catch(function (error) {
            console.error("Error writing document: ", error);
        });

})


// Delete a order 
app.delete('/order/:orderId', (req, res) => {
    firebaseHelper.firestore
        .deleteDocument(db, ordersCollection, req.params.orderId);
    res.send('order is deleted');
})