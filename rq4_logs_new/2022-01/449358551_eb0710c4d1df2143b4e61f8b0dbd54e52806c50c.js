const { response } = require('express');
const express = require('express');
const bodyParser = require("body-parser");
const fs = require('fs');
const merkle = require('./merkle');

const router = express.Router()
const app = express();

app.use(express.static('static'))
app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());


app.get('/', (request, response) => {
    fs.readFile('./home.html', 'utf-8', (err, html) => {

        if(err) {
            response.status(500).send('sorry, out of order');
            console.log(err)
        }

        response.send(html);
    });
});

app.post('/address', (request, response) => {
    const account = request.body.account;
    console.log(""+ account + " has logged in");
    const mintInfo = merkle.allowlist[account];
    if (!mintInfo) {
        response.send('false')
        return
    }
    const [allowedAmount, free] = mintInfo;
    const leaf = merkle.generateLeaf(account, allowedAmount, free);
    const proof = merkle.merkleTree.getHexProof(leaf);
    const output = {
        'allowedAmount': allowedAmount,
        'free': free,
        'proof': proof
    }
    response.send(JSON.stringify(output))
});


app.listen(process.env.PORT || 8070, () => console.log('app running on port 8070'));