const express = require('express');
const app = express();
const Joi = require('joi');

app.use(express.json());

const courses = [
    { id: 1, name: 'course1' },
    { id: 2, name: 'course2' },
    { id: 3, name: 'course3' },
];



//get block

app.get('/', (req, res) => {
    res.send('Here we go');
});

app.get('/api/courses', (req, res) => {
    res.send(courses);
});

app.get('/api/courses/:id', (req, res) => {
    const course = courses.find(c => c.id === parseInt(req.params.id));
    if (!course) {
        res.status(404).send('The course with the given id was not found');
        return;
    }
    res.send(course);
});



//post block

app.post('/api/courses/', (req, res) => {

    /*if(!req.body.name || req.body.name.length < 3){
        res.status(404).send('Name is required and should be minimum 3 characters');
        return; 
    }*/
    const { error } = validateCourse(req.body);
    if (error) {
        res.status(400).send(error.details[0].message);
        return;
    }


    const course = {
        id: courses.length + 1,
        name: req.body.name
    };
    courses.push(course);
    res.send(course);
});



//put block

app.put('/api/courses/:id', (req, res) => {
    const course = courses.find(c => c.id === parseInt(req.params.id));
    if (!course) {
        res.status(404).send('The course with the given id was not found');
        return;
    }
    const { error } = validateCourse(req.body);
    if (error) {
        res.status(400).send(error.details[0].message);
        return;
    }

    course.name = req.body.name;
    res.send(course);
});



//delete block

app.delete('/api/courses/:id', (req, res) => {
    const course = courses.find(c => c.id === parseInt(req.params.id));
    if (!course) {
        res.status(404).send('The course with the given id was not found');
        return;
    }
    const index = courses.indexOf(course);
    courses.splice(index, 1);
    res.send(course);
});


function validateCourse(course) {
    const schema = {
        name: Joi.string().min(3).required()
    };

    return Joi.validate(course, schema);

}



const port = process.env.PORT || 8080;
app.listen(port, () => console.log(`listing to port ${port}...`));