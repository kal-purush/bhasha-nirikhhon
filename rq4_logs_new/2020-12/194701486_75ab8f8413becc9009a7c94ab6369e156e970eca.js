module.exports = (app, Author) => {
    app.get('/authors', (req, res) => {
        Author.find().exec(function (err, authors) {
            if (err) {
                console.log(err);
            } else {
                return res.send(authors);
            }
        });
    });
    
    app.get('/authors/:_id', (req, res) => {
        Author.findOne({ _id: req.params._id }).exec(function (err, author) {
            if (err) {
                console.log(err);
            } else {
                return res.send(author);
            }
        });
    });
    
    app.post('/authors', (req, res) => {
        var author = new Author(req.body);
    
        author.save(function (err, author) {
            if (err) {
                console.log(err);
            } else {
                return res.send(author);
            }
        });
    });
    
    app.put('/authors/:_id', (req, res) => {
        Author.findOne({ _id: req.params._id }).exec(function (err, author) {
            if (err) {
                console.log(err);
            } else {
    
                author.title = req.body.title;
                author.author = req.body.author;
    
                author.save(function (err, author) {
                    if (err) {
                        console.log(err);
                    } else {
                        return res.send(author);
                    }
                });
            }
        });
    });
    
    app.delete('/authors/:_id', (req, res) => {
        Author.findOne({ _id: req.params._id }).exec(function (err, author) {
            if (err) {
                console.log(err);
            } else {
                author.remove(function (err, author) {
                    if (err) {
                        console.log(err);
                    } else {
                        return res.send(author);
                    }
                });
            }
        });
    });
}