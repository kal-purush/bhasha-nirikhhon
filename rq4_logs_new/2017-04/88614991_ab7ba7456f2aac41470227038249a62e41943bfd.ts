import * as auth from '../../auth/auth.service';
import Post from './post.model';
import config from '../../config/config';

export default {
    index(req, res) {

    },
    show(req, res) {

    },
    create(req, res) {
        let post = {
            category: req.body.category,
            ad_title: req.body.ad_title,
            brand_name: req.body.brand_name,
            brand_description: req.body.brand_description,
            posted_by: req.user._id,
            price: req.body.price
        }
        console.log('post', post);
        Post.create(post)
            .then(post => {
                console.log('post created', post);
                return res.json({ success: true, data: 'Post created', error: null });
            })
            .catch(err => {
                console.error('err', err);
                return res.json({ success: false, data: null, error: err });
            });

    },
    update(req, res) {

    },
    delete(req, res) {

    }
};