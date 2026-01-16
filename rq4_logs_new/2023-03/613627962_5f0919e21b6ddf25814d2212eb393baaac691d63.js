const express = require('express');

const studentdata = require('./studendb')


const app = express();

app.get('/api/info/:code', (req, res) => {
    const { code } = req.params 
    const studencode = studentdata[code];

    let infostudent = (studencode !== undefined && studencode !== '' )? studencode : 'No info' 

    res.send(infostudent)
    
});

app.listen(3000, () => {
  console.log('La aplicación está corriendo en http://localhost:3000');
});