import * as Sequelize from 'sequelize';
import * as Model from '../app/models/sequelize.model';
import { BookModel, BookInstance } from '../app/models/book.model';
import { WriterModel, WriterInstance, WriterId } from '../app/models/writer.model';
import { CatModel } from '../app/models/cat.model';


let bookModel = Model.book;
let writerModel = Model.writer;

    // tao moi table book va them 3 du lieu
    bookModel.sync({ force: true }).then(() => {
        bookModel.bulkCreate([
            {
                name: 'book1'
            },
            {
                name: 'book2'
            },
            {
                name: 'book3'
            }
        ])
    }).then(() => {
        // tao moi table writer va them 3 du lieu
        writerModel.sync({ force: true }).then(() => {
            writerModel.bulkCreate([
                {
                    name: 'hong duc'
                },
                {
                    name: 'Jimmy'
                },
                {
                    name: 'Jane the savior'
                }
            ])
        })

    })


    // table nay se dong bo voi table co san trong postgresql
    // catModel.sync().then(() => {
    //     catModel.bulkCreate([
    //         {
    //             name: 'cat orm'
    //         },
    //         {
    //             name: 'cat orm 2'
    //         }
    //     ])
    // })    