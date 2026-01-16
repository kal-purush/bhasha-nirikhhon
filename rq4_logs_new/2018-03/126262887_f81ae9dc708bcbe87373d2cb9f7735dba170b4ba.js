var Author = require('../models/author');

//Display list of all authors.
exports.author_list = function(req, res){
    res.send('NOT IMPLEMENTED: Author detail');
};

//Display detail page for a specific Author
exports.author_detail = function(req, res){
    res.send('NOT IMPLEMENTED: author_detail'+ req.params.id);
};

//Display author create form on GET
exports.author_create_get = function(req, res){
    res.send('NOT IMPLEMENTED: author_create_get');
};

//Display author create on POST
exports.author_create_post = function(req, res){
    res.send('NOT IMPLEMENETED: author_create_post');
};

//Display author delete on GET
exports.author_delete_get = function(req, res){
    res.send('NOT IMPLEMNTED: author_delete_get');
};

//Display author delete on POST
exports.author_delete_post = function(req, res){
    res.send('NOT IMPLEMENETD: author_delete_post');
};

//Display author update form on GET
exports.author_update_get = function(req, res){
    res.send('NOT IMPLEMENTED: author_update_get');
};

//Handle author update on POST
exports.author_update_post = function(req, res){
    res.send('NOT IMPLEMENTED: author_update_post');
};


