Meteor.startup(function () {
  if (Todos.find().count() === 0) {

  var todos = [
      {
        'title': 'Implement data model for ToDo-App',
        'responsible': 'Sebastian',
        'completed': false
      },
      {
        'title': 'Test new functionality',
        'responsible': 'Sebastian',
        'completed': true
      },
      {
        'title': 'Review technical concept',
        'responsible': 'Sebastian',
        'completed': false
      }
    ];

  for (var i = 0; i < todos.length; i++)
      Todos.insert(todos[i]);
  }
});