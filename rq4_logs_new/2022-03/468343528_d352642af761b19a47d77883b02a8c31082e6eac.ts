import { UpdateBookInput } from './../../dto/update-book.input';
import { CreateBookInput } from './../../dto/create-book.input';
import { Book } from '../../entities/book.entity';

const booksList: Book[] = [
  new Book({
    _id: '55419226-3fc6-41be-a9b5-99a3dc3ed030',
    titulo: 'Jantar Secreto',
    autor: 'Rafael Montes',
    user: ' 591b6e82-c353-4c14-8546-42cccabcf18c',
    genero: 'Ficção',
    anoDeLancamento: '2016',
  }),
  new Book({
    _id: 'fa9a0a41-5806-4d8c-b366-6efc634be215',
    titulo: 'Suicidas',
    autor: 'Rafael Montes',
    user: '5894d107-313a-4972-823e-512d5541f70c',
    genero: 'Ficção',
    anoDeLancamento: '2016',
  }),
  new Book({
    _id: '6504858d-da3d-4cb3-bdc1-660a1527ca38',
    titulo: 'Stalin. História Crítica de Uma Lenda Negra',
    autor: 'Domenico Losurdo',
    user: 'c1489c3d-fc6f-4b33-9e49-322b99101436',
    genero: 'Biografia',
    anoDeLancamento: '2010',
  }),
];

const createBookInput: CreateBookInput = {
  titulo: 'Dias Perfeitos',
  autor: 'Rafael Montes',
  genero: 'Ficção',
  anoDeLancamento: '2014',
};

const bookupdate: UpdateBookInput = {
  id: '55419226-3fc6-41be-a9b5-99a3dc3ed030',
  titulo: 'Neuromancer',
  autor: 'Wilson Gibson',
  genero: 'SciFi',
  anoDeLancamento: '1986',
};

export default {
  giveMeOneValidBook: () => booksList[0],
  giveMeValidBooks: () => booksList,
  giveMeValidCreateInput: () => createBookInput,
  giveMeValidUpdateInput: () => bookupdate,
};