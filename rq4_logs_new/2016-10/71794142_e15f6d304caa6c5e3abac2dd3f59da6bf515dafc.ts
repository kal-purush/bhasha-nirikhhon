import { Router, Response } from 'express';
import { Pool, Client, QueryResult } from 'pg';
import { Promise } from 'es6-promise';
import { WriterModel } from '../models/writer.model';
import { BookModel } from '../models/book.model';
import * as Model from '../models/sequelize.model';


export = () => {
    let router = Router();
    let writerModel = Model.writer;
    let bookModel = Model.book;


    let handleError = (error: Error, response: Response): Promise<Error> => {
        response.status(500).send(error.message);
        return Promise.reject(error);
    }



    router.route('/writer')


        .get((req, res) => {
            let id = req.query.id;
            if (id) {
                writerModel.findById(id).then(w => {
                    if (w) {
                        res.json(w);
                    } else {
                        res.status(404).send('writer not found with id=' + id);
                    }

                })
                    .catch(error => handleError(error, res));
            } else {
                writerModel.findAll().then(ws => {
                    res.json(ws);
                })
                    .catch(error => handleError(error, res));
            }
        })


        .post((req, res) => {
            writerModel.create(req.body).then(w => {
                res.json(w);
            })
                .catch(err => handleError(err, res));
        })

        .put((req, res) => {
            let id = req.query.id;
            writerModel.findById(id).then(w => {
                if (w) {
                    w.update(req.body).then(w => res.json(w))
                } else {
                    res.status(404).send('not found writer with id=' + id);
                }
            })
                .catch(err => handleError(err, res));
        })


    router.get('/whatBookWriterWrite', (req, res) => {
        let id = req.query.id;
        writerModel.findById(id).then(w => {
            bookModel.findById(w.bookId).then(b => {
                if(b){
                    res.json(b);
                }else{
                    res.status(404).send('writer not write any book');
                }
            })
        })
        .catch(error => handleError(error,res));
    });

    return router;
}