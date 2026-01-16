import * as express from 'express';
import Course from '../models/course';

let router = express.Router();

router.get('/', (req, res) => {
  Course.find().then((courses) => res.json(courses));
});

router.post('/', (req, res) => {
  let newCourse = new Course();
  newCourse.name = req.body.name;
  newCourse.icon = req.body.icon;
  newCourse.save().then((createdCourse) => res.json(createdCourse));
});

router.get('/:id', (req, res) => {
  Course.findById(req.params.id)
  .then((foundCourse) => res.json(foundCourse));
});

router.post('/:id', (req, res) => {
  Course.findById(req.params.id).then((foundCourse) => {
    foundCourse.name = req.body.name;
    foundCourse.icon = req.body.icon;
    foundCourse.save().then((savedCourse) => res.json(savedCourse));
  });
});

router.delete('/:id', (req, res) => {
  Course.remove({_id: req.params.id})
  .then((deletedcourse) => res.json(deletedcourse))
  .catch((err) => res.json(err));
});

export default router;