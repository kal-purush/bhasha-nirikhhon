const Book = require('../models/book')

module.exports = {
  find_books: async (req, res) => {
    const books = await Book.find().catch((err) => {
      res.send(err)
      console.error(err)
    })
    return res.json(books)
  },

  create_book: async (req, res) => {
    const book = new Book()
    book.name = req.body.name
    book.price = req.body.price
    book.is_public = req.body.is_public
    await book.save().catch((err) => {
      res.send(err)
      console.error(err)
    })
    return res.json(book)
  },

  find_book: async (req, res) => {
    const book = await Book.findById(req.params.id).catch((err) => {
      res.send(err)
      console.error(err)
    })
    return res.json(book)
  },

  update_book: async (req, res) => {
    const book = await Book.findById(req.params.id).catch((err) => {
      res.send(err)
      console.error(err)
    })
    book.name = req.body.name
    book.price = req.body.price
    book.is_public = req.body.is_public
    await book.save()
    return res.json(book)
  },

  delete_book: async (req, res) => {
    await Book.deleteOne({
      _id: req.params.id
    }).catch((err) => {
      res.send(err)
      console.error(err)
    })
    return res.json({ message: 'Successfully deleted' })
  }
}