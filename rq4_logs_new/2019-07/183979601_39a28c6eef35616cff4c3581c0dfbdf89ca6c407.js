const moment = require('moment')
const { postExists } = require('./utils')

module.exports = (plop) => {
  plop.setGenerator('add post', {
    prompts: [
      {
        type: 'input',
        name: 'title',
        message: 'Please input your post tile.',
        validate: (value) => {
          if (/.+/.test(value)) {
            return postExists(value) ? 'A post with this name is already exists' : true
          }
          return 'post name is required !'
        }
      },
      {
        type: 'input',
        name: 'description',
        message: 'Please add your post description below.',
        default: 'This is a new post!'
      }
    ],
    actions: (data) => {
      /* Get current date */
      data.date = moment().format('YYYY-MM-DD')
      return [
        {
          type: 'add',
          path: '../content/blog/{{dashCase title}}/index.md',
          templateFile: './post.template'
        }
      ]
    }
  })
}