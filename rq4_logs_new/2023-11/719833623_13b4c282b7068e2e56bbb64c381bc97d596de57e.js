'use strict'
const Todo = use('App/Models/Todo');
const CloudinaryService = use('App/Services/CloudinaryService');
const Helpers = use('Helpers')


class TodoController {
  async index({ request, response, view }) {
    const todos = await Todo.all();
    return view.render('index', { todos: todos.rows })
  }

  create({ request, response, view }) {
    return view.render('create')
  }

  async store({ request, response, view, session }) {

    const { title, description } = request.all();
    const file = request.file('img');
    try {
      const todo = new Todo();

      todo.title = request.input('title');
      todo.description = request.input('description');

      if (file !== null) {
        const image = request.file('img', {
          types: ['image'],
          size: '2mb'
        })
        const filename = new Date().getTime() + '.' + image.subtype
        await image.move(Helpers.publicPath('images'), {
          name: `${filename}`,
          overwrite: true
        })
        todo.img = `images/${filename}`
      }

      await todo.save();
      session.flash({ notification: 'Successfully create!' });
      return response.route('Todo.index')

    } catch (error) {
      session.flash({ notification: 'Error Uploading Image' });
      return response.route('Todo.index')
    }
  }

  async edit({ request, response, view, params }) {
    const id = params.id;
    const todo = await Todo.find(id);

    return view.render('edit', { todo: todo })
  }

  async update({ request, response, view, params, session }) {

    const { title, description } = request.all();
    const file = request.file('img');
    try {
      const id = params.id;
      const todo = await Todo.find(id);

      todo.title = request.input('title');
      todo.description = request.input('description');

      if (file !== null) {
        const image = request.file('img', {
          types: ['image'],
          size: '2mb'
        })
        const filename = new Date().getTime() + '.' + image.subtype
        await image.move(Helpers.publicPath('images'), {
          name: `${filename}`,
          overwrite: true
        })
        todo.img = `images/${filename}`
      }

      await todo.save();
      session.flash({ notification: 'Successfully Update!' });
      return response.route('Todo.index')
    } catch (error) {
      session.flash({ notification: 'Error Uploading Image' });
      return response.route('Todo.index')
    }

  }

  async delete({ request, response, view, params, session }) {

    const id = params.id;
    const todo = await Todo.find(id);

    await todo.delete();

    session.flash({ notification: 'Successfully delete!' });
    response.redirect('/')
  }
}

module.exports = TodoController