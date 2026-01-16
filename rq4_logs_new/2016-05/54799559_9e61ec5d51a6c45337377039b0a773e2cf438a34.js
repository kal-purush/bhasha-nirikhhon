import React from 'react';
import { reduxForm } from 'redux-form';

const AddEnemy = (props) => {
  const { fields: { name, hp, bonus }, handleSubmit } = props;
  return (
    <form onSubmit={handleSubmit}>
      <div className="row">
        <label>Name:</label>
        <input type="text" {...name} />
      </div>
      <div className="row">
        <label>HP:</label>
        <input type="number" min="0" max="1000" {...hp} />
      </div>
      <div className="row">
        <label>Initiative:</label>
        <input type="number" min="0" max="100" {...bonus} />
      </div>
      <div className="row">
        <button type="submit">Add</button>
      </div>
    </form>
  );
};

const AddEnemyForm = reduxForm({
  form: 'addEnemy',
  fields: ['name', 'hp', 'bonus']
})(AddEnemy);

export default AddEnemyForm;