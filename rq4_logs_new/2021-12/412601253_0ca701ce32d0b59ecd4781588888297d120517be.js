const Course = require('../models/Course');
const { MongooseToObject } = require('../../util/mongoose');

class CourseController {
    // [GET] /courses/:slug
    show(req, res, next) {
        Course.findOne({ slug: req.params.slug })
            .then((course) => {
                res.render('courses/show', {
                    course: MongooseToObject(course),
                });
            })
            .catch(next);
        // res.send('Course Detail - ' + req.params.slug);
    }

    // [GET] /courses/create
    create(req, res, next) {
        res.render('courses/create');
    }
    // [POST] /courses/store
    store(req, res, next) {
        // const formData = {...req.body};
        // formData.image = `https://i.ytimg.com/vi/${req.body.videoId}/hq720.jpg?sqp=-oaymwEcCNAFEJQDSFXyq4qpAw4IARUAAIhCGAFwAcABBg==&rs=AOn4CLDK5d8cae7_XAK4Qe5uWyAxH9jnKw`
        req.body.image = `https://i.ytimg.com/vi/${req.body.videoId}/hq720.jpg?sqp=-oaymwEcCNAFEJQDSFXyq4qpAw4IARUAAIhCGAFwAcABBg==&rs=AOn4CLDK5d8cae7_XAK4Qe5uWyAxH9jnKw`;

        const course = new Course(req.body);
        course
            .save()
            .then(() => {
                res.redirect('/me/stored/courses');
            })
            .catch(next);

        // AutoIncrement
        // Course.findOne({})
        //         .sort({_id: -1}) //1 là asc, -1 desc
        //         .then(latestCourse =>{

        //             // return res.json(latestCourse);

        //             req.body._id = latestCourse._id +1;
        //             const course = new Course(req.body);
        //             course
        //                 .save()
        //                 .then(() => {
        //                     res.redirect('/me/stored/courses');
        //                 })
        //                 .catch(next);
        //         });
    }
    // [GET] /courses/:id/edit
    edit(req, res, next) {
        Course.findById(req.params.id)
            .then((course) => {
                res.render('courses/edit', {
                    course: MongooseToObject(course),
                });
            })
            .catch(next);
    }

    // [PUT] /courses/:id/
    update(req, res, next) {
        req.body.image = `https://i.ytimg.com/vi/${req.body.videoId}/hq720.jpg?sqp=-oaymwEcCNAFEJQDSFXyq4qpAw4IARUAAIhCGAFwAcABBg==&rs=AOn4CLDK5d8cae7_XAK4Qe5uWyAxH9jnKw`;
        Course.updateOne({ _id: req.params.id }, req.body)
            .then(() => {
                res.redirect('/me/stored/courses');
            })
            .catch(next);
    }

    // [DELETE] /courses/:id/
    delete(req, res, next) {
        Course.delete({ _id: req.params.id })
            .then(() => {
                res.redirect('back');
            })
            .catch(next);
    }

    // [DELETE] /courses/:id/force
    destroy(req, res, next) {
        Course.deleteOne({ _id: req.params.id })
            .then(() => {
                res.redirect('back');
            })
            .catch(next);
    }

    // [PATCH] /courses/:id/restore
    restore(req, res, next) {
        Course.restore({ _id: req.params.id })
            .then(() => {
                res.redirect('back');
            })
            .catch(next);
    }

    // [POST] /courses/handle-form-action
    handleFormAction(req, res, next) {
        switch (req.body.action) {
            case 'delete':
                Course.delete({ _id: { $in: req.body.courseIds } })
                    .then(() => {
                        res.redirect('back');
                    })
                    .catch(next);
                break;
            case 'restore':
                Course.restore({ _id: { $in: req.body.courseIds } })
                    .then(() => {
                        res.redirect('back');
                    })
                    .catch(next);
                break;
            case 'destroy':
                Course.deleteOne({ _id: { $in: req.body.courseIds } })
                    .then(() => {
                        res.redirect('back');
                    })
                    .catch(next);
                break;
            default:
                res.redirect('back');
                break;
        }
    }
}

// GET, POST, PUT, PATCH, DEL , OPTIONS, HEAD

module.exports = new CourseController();

// Call back--------------
// Course.find({}, function(err, courses) {
//     if(!err){
//         res.json(courses);
//         // return;
//     }
//     else{
//         next(err);
//         // res.status(400).json({error: 'ERROR!!!!'});
//     }
// });