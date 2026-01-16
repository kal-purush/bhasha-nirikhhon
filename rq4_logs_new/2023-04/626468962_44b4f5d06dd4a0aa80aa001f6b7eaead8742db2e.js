const express = require("express");
const axios = require('axios').default;

const PORT = process.env.PORT || 3000;
const app = express();
const db = require("./models");
const User = db.users;


db.sequelize.sync({ alter: true })
    .then(() => {
        console.log("Synced db.");
    })
    .catch((err) => {
        console.log("Failed to sync db: " + err.message);
    });


//получение записи с указанным фамилией
app.get('/user/:lastName', async (req, res) => {
    let lastName = req.params.lastName
    let data = await User.findAll({
        where: { lastName },
        include: [db.locations, db.credentials, db.pictures]
    });
    return res.send(data);
});


app.get('/', async (req, res) => {
    let response = await axios.get('https://randomuser.me/api/');
    let data = response.data.results[0];

    User.create({
        gender: data.gender,
        title: data.name.title,
        firstName: data.name.first,
        lastName: data.name.last,
        email: data.email,
        dateOB: data.dob.date,
        ageFB: data.dob.age,
        dateOR: data.registered.date,
        ageFR: data.registered.age,
        phone: data.phone,
        cell: data.cell,
        idName: data.id.name,
        idValue: data.id.value,
        nat: data.nat,

        location: {
            streetNumber: data.location.street.number,
            streetName: data.location.street.name,
            city: data.location.city,
            state: data.location.state,
            country: data.location.country,
            postcode: data.location.postcode,
            latitude: data.location.coordinates.latitude,
            longitude: data.location.coordinates.longitude,
            timezoneOffset: data.location.timezone.offset,
            timezoneDescription: data.location.timezone.description,
        },
        credential: {
            ...data.login
        },
        picture: {
            ...data.picture
        },

    },
        {
            include: [db.locations, db.credentials, db.pictures]
        });

    res.send('new record was created!');
});


app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}.`);
});