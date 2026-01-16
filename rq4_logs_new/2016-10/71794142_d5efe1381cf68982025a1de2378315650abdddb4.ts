import * as Sequelize from 'sequelize';
import * as Writer from './writer.model';
import * as Author from './author.model';

/**
 * tượng trưng cho cột của table
 */
export interface BookAttribute {
    id?: number;
    name?: string;
    writers?: Writer.WriterModel[];
}

/**
 * tượng trưng cho id
 */
export interface BookId { BookId: number }


/**
 * tượng trưng cho 1 row trong table
 */
export interface BookInstance extends Sequelize.Instance<BookAttribute>, BookAttribute {
    getWriters: Sequelize.BelongsToManyGetAssociationsMixin<Writer.WriterInstance>;
    addWriter: Sequelize.BelongsToManyAddAssociationMixin<Writer.WriterInstance,Writer.WriterId,Author.AuthorAttribute>;
    addWriters: Sequelize.BelongsToManyAddAssociationsMixin<Writer.WriterInstance,Writer.WriterId,Author.AuthorAttribute>;
 }


/**
 * tượng trưng 1 cái table
 */
export interface BookModel extends Sequelize.Model<BookInstance, BookAttribute> {}