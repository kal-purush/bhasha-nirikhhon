const { nanoid } = require('nanoid');
const notes = require('./notes');

const getAllNotesHandler = (req, h) => {
  const response = h
    .response({
      status: 'success',
      message: 'berhasil mengambil notes',
      data: {
        notes,
      },
    })
    .code(200);

  return response;
};

const getSpecificNoteHandler = (req, h) => {
  const { id } = req.params;

  const foundedNoteIdx = notes.findIndex((note) => note.id === id);

  if (foundedNoteIdx !== -1) {
    const response = h
      .response({
        status: 'success',
        message: `berhasil mengambil catatan dengan id ${id}`,
        data: {
          note: notes[foundedNoteIdx],
        },
      })
      .code(200);
    return response;
  }

  const response = h
    .response({
      status: 'fail',
      message: `Tidak ada catatan dengan id ${id}`,
    })
    .code(404);
  return response;
};

const addNoteHandler = (req, h) => {
  const { title, body, tags } = req.payload;

  const id = nanoid(16);
  const createdAt = new Date().toISOString();
  const updatedAt = createdAt;

  const newNotes = {
    id,
    title,
    body,
    tags,
    createdAt,
    updatedAt,
  };

  notes.push(newNotes);

  const isSuccess = notes.filter((note) => note.id === id).length > 0;

  if (isSuccess) {
    const response = h
      .response({
        status: 'success',
        message: 'catatan berhasil ditambahkan',
        data: {
          noteId: id,
        },
      })
      .code(201);

    return response;
  }

  const response = h
    .response({
      status: 'failed',
      message: 'catatan gagal ditambahkan',
    })
    .code(500);

  return response;
};

const editSpecificNote = (req, h) => {
  const { id } = req.params;

  const { title, tags, body } = req.payload;

  const foundedNoteIdx = notes.findIndex((note) => note.id === id);

  if (foundedNoteIdx !== -1) {
    const updatedAt = new Date().toISOString();

    notes[foundedNoteIdx] = {
      ...notes[foundedNoteIdx],
      title,
      tags,
      body,
      updatedAt,
    };

    const response = h
      .response({
        status: 'success',
        message: `berhasil memperbarui catatan dengan id ${id}`,
      })
      .code(200);

    return response;
  }

  const response = h
    .response({
      status: 'fail',
      message: `Tidak ada catatan dengan id ${id}`,
    })
    .code(404);
  return response;
};

const deleteNote = (req, h) => {
  const { id } = req.params;

  const foundedNoteIdx = notes.findIndex((note) => note.id);

  if (foundedNoteIdx !== -1) {
    notes.splice(foundedNoteIdx, 1);
    const response = h
      .response({
        status: 'success',
        message: `berhasil menghapus catatan dengan id ${id}`,
      })
      .code(200);

    return response;
  }

  const response = h
    .response({
      status: 'fail',
      message: `Tidak ada catatan dengan id ${id}`,
    })
    .code(404);
  return response;
};

module.exports = {
  addNoteHandler,
  getAllNotesHandler,
  getSpecificNoteHandler,
  editSpecificNote,
  deleteNote,
};