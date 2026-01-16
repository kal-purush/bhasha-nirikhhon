
/*

Model -> defines types and relationships

Schema -> collection of models, defines overall structure
 - Model

Table -> collection of data for a model
 - Model

DB -> contains all data and data manipulations
 - Schema
 - Table
 - Model

*instance -> access to 

*/




render() {
  const {schema} = this.props;

  // Get model unattached to database.
  let Task = schema.Task;

  // Get model attached to database.
  let db = new DB( schema );
  let Task = db.Task;

  // Get 

  // Get instance.
  let task = Task.get( {id: 1} );
  let task = Task.get( {id: 1, name: 'hello'} );
}

const {taskObject} = this.props;
taskObject.title = 'Some title';
taskObject.parent = anotherTaskObject;
taskObject.components.all(); // -> []
taskObject.components.add( [anotherTaskObject, yetAnother] );
taskObject.save()