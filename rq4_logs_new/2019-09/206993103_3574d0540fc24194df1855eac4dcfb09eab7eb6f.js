const express = require('express');
const fileUpload = require('express-fileupload');
const { createFile } = require('../extra/file_util');
const { filter } = require('../extra/filter_data');
const { showOne, errorResponse } = require('../extra/responser');
const User = require('../models/User');

const app = express();
app.use(fileUpload({ parseNested: true }));

app.post('/', (req, res) => {
    let body = req.body;
    let data = filter(body, User);

    if (body.name) {
        data.name = {
            firstname: body.name.firstname,
            lastname: body.name.lastname
        };
    }

    let user = new User(data);

    user.save(async (err, user) => {
        if (err) {
            return errorResponse(res, err, 400);
        }

        if (Object.keys(req.files).length != 0 && req.files.image) {
            let url = createFile('user', req.files.image);

            user.photo = url;
            await user.save();
        }

        return showOne(res, user, 201);
    });
});

module.exports = app;