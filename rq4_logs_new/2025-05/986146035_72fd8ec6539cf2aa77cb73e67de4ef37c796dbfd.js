const service = require('../services/booksService');

const addBookHandler = (request, h) => {
  const {
    name, year, author, summary,
    publisher, pageCount, readPage, reading,
  } = request.payload;

  if (!name) {
    return h.response({
      status: 'fail',
      message: 'Gagal menambahkan buku. Mohon isi nama buku',
    }).code(400);
  }

  if (readPage > pageCount) {
    return h.response({
      status: 'fail',
      message: 'Gagal menambahkan buku. readPage tidak boleh lebih besar dari pageCount',
    }).code(400);
  }

  const bookId = service.addBook({
    name, year, author, summary,
    publisher, pageCount, readPage, reading,
  });

  return h.response({
    status: 'success',
    message: 'Buku berhasil ditambahkan',
    data: { bookId },
  }).code(201);
};

const getAllBooksHandler = (request, h) => {
  const { name, reading, finished } = request.query;

  let filtered = service.getBooksRaw(); 

  if (name) {
    const key = name.toLowerCase();
    filtered = filtered.filter((b) =>
      b.name.toLowerCase().includes(key)
    );
  }

  if (reading !== undefined) {
    const isReading = reading === '1';
    filtered = filtered.filter((b) => b.reading === isReading);
  }

  if (finished !== undefined) {
    const isFinished = finished === '1';
    filtered = filtered.filter((b) => b.finished === isFinished);
  }

  const books = filtered.map(({ id, name: nm, publisher }) => ({
    id,
    name: nm,
    publisher,
  }));

  return h.response({
    status: 'success',
    data: { books },
  }).code(200);
};


const getBookByIdHandler = (request, h) => {
  const { bookId } = request.params;
  const book = service.getBookById(bookId);

  if (!book) {
    return h.response({
      status: 'fail',
      message: 'Buku tidak ditemukan',
    }).code(404);
  }

  return h.response({
    status: 'success',
    data: { book },
  }).code(200);
};

const updateBookHandler = (request, h) => {
  const { bookId } = request.params;
  const {
    name, year, author, summary,
    publisher, pageCount, readPage, reading,
  } = request.payload;

  if (!name) {
    return h.response({
      status: 'fail',
      message: 'Gagal memperbarui buku. Mohon isi nama buku',
    }).code(400);
  }

  if (readPage > pageCount) {
    return h.response({
      status: 'fail',
      message: 'Gagal memperbarui buku. readPage tidak boleh lebih besar dari pageCount',
    }).code(400);
  }

  const success = service.updateBook(bookId, {
    name, year, author, summary,
    publisher, pageCount, readPage, reading,
  });

  if (!success) {
    return h.response({
      status: 'fail',
      message: 'Gagal memperbarui buku. Id tidak ditemukan',
    }).code(404);
  }

  return h.response({
    status: 'success',
    message: 'Buku berhasil diperbarui',
  }).code(200);
};

const deleteBookHandler = (request, h) => {
  const { bookId } = request.params;
  const success = service.deleteBook(bookId);

  if (!success) {
    return h.response({
      status: 'fail',
      message: 'Buku gagal dihapus. Id tidak ditemukan',
    }).code(404);
  }

  return h.response({
    status: 'success',
    message: 'Buku berhasil dihapus',
  }).code(200);
};

module.exports = {
  addBookHandler,
  getAllBooksHandler,
  getBookByIdHandler,
  updateBookHandler,
  deleteBookHandler,
};