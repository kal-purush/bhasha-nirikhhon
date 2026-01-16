exports.render = function(req, res) {
    res.render('forum', {
    	title: 'Admin Forum',
    	user: req.user ? req.user.username : ''
    });
};