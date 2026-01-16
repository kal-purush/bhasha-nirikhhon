var routes = require("express").Router();
let libraryBookCopiesDao = require('../dao/libraryBookCopiesDao');

routes.patch('/api/librarian/branches/:branchId(\\d+)/books/:bookId(\\d+)/copies', function(req, res){
    let libraryBookCopies = { bookId: req.params.bookId,
        branchId: req.params.branchId,
        noOfCopies: req.body.noOfCopies
    };
    libraryBookCopiesDao.updateBookCopies(libraryBookCopies, function(err, result){
        if (err){
            res.status(400);
            res.send("Bad Request");
        }
        console.log("Book Copies: ", result.message);
        libraryBookCopiesDao.getAllCopiesOfBookInBranch(req.params.branchId, req.params.bookId, function(err1, result1){
            if (err1){
                res.status(500);
                res.send("Error retrieving book copies");
            }
            res.status(200);
            let st = result1.length + " Records Returned";
            console.log(st);
            res.send(result1);
        });
    });
});

routes.post('/api/librarian/branches/:branchId(\\d+)/books', function(req, res){
    let libraryBookCopies = { bookId: req.body.bookId,
        branchId: req.params.branchId,
        noOfCopies: req.body.noOfCopies
    };
    libraryBookCopiesDao.saveBranchBook(libraryBookCopies, function(err, result){
        if (err){
            res.status(400);
            res.send("Adding new book to branch failed");
        }
        libraryBookCopiesDao.getAllCopiesOfBookInBranch(req.params.branchId, libraryBookCopies.bookId, function(err1, result){
            if (err){
                res.status(500);
                res.send("Error retrieving book copies");
            }
            res.status(200);
            let st = result.length + " Records Returned";
            console.log(st);
            res.send(result);
        });
    });
});

routes.get('/api/librarian/branches/:branchId(\\d+)/books/:bookId(\\d+)/copies', function(req, res){
    libraryBookCopiesDao.getAllCopiesOfBookInBranch(req.params.branchId, req.params.bookId, function(err, result){
        if (err){
            res.status(400);
            res.send("Error retrieving book copies");
        }
        res.status(200);
        let st = result.length + " Records Returned";
        console.log(st);
        res.send(result);
    });
});

module.exports = routes;