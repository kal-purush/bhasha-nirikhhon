const Todo = require('../model/Todo');

module.exports.getTodo = async (req, res) => {
  try {
    const todos = await Todo.findAll();
    res.send(todos);
  } catch (error) {
    res.status(422).send({ answer: error });
  }
};

module.exports.postTodo = async (req, res) => {
  const { todoItem } = req.body;
  const errorsArray = [];

  if (!todoItem.trim()) {
    errorsArray.push('You must enter a todo item!');

    return res.status(422).send({ answer: errorsArray });
  }

  try {
    const newTodo = await Todo.create({
      todoItem,
    });

    return newTodo && (await this.getTodo(req, res));
  } catch (error) {
    return res.status(422).send({ answer: error });
  }
};

module.exports.editTodoById = async (req, res) => {
  const { todoItem, checked } = req.body;
  const { id } = req.params;
  const errorsArray = [];
  const validEdit = {};

  if (!todoItem && !checked) {
    errorsArray.push('You must change at least one field!');
  }

  if (!errorsArray.length) {
    if (todoItem) {
      if (!todoItem.trim()) {
        errorsArray.push('Todo item must not be empty!');
      } else {
        validEdit.todoItem = todoItem;
      }
    }

    if (checked) validEdit.checked = checked;
  } else {
    return res.status(422).send({ answer: errorsArray });
  }

  try {
    const [change] = await Todo.update(validEdit, { where: { id } });
    if (change) return await this.getTodo(req, res);
    return res.status(404).send({ answer: 'Todo item does not exist!' });
  } catch (error) {
    return res.status(422).send({ answer: error });
  }
};

module.exports.deleteTodoById = async (req, res) => {
  const { id } = req.params;

  if (!String(id).trim())
    return res.status(422).send({ answer: 'Invalid ID has been passed!' });

  try {
    const remove = await Todo.destroy({ where: { id } });
    if (remove) return await this.getTodo(req, res);
    return res.status(404).send({ answer: 'Todo item does not exist' });
  } catch (error) {
    return res.status(422).send({ answer: error });
  }
};