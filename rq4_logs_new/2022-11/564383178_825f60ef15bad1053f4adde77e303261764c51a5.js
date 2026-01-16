import { Col, Divider, Input } from 'antd';
import { useState } from 'react';

export function AddFoodForm(props) {
  const [form, setForm] = useState({
    name: '',
    image: '',
    calories: 0,
    servings: 0,
  });

  const handleChanges = (e) => {
    setForm({ ...form, [e.target.name]: e.target.value });
  };

  const handleSubmit = (e) => {
    e.preventDefault();

    props.addFood(form);

    setForm({
      name: '',
      image: '',
      calories: 0,
      servings: 0,
    });
  };

  return (
    <Col span={24}>
      <Divider>Add Food Entry</Divider>
      <form
        onSubmit={handleSubmit}
        style={{
          width: 480,
          height: 300,
          margin: '10px auto',
          textAlign: 'left',
        }}
      >
        <label htmlFor="name">Name</label>
        <Input
          name="name"
          value={form.name}
          type="text"
          onChange={handleChanges}
        />

        <label htmlFor="image">Image</label>
        <Input
          placeholder="https://imgur.com/0WkdgkE.jpg"
          name="image"
          value={form.image}
          type="text"
          onChange={handleChanges}
        />

        <label htmlFor="calories">Calories</label>
        <Input
          name="calories"
          value={form.calories}
          type="number"
          onChange={handleChanges}
        />

        <label htmlFor="servings">Servings</label>
        <Input
          name="servings"
          value={form.servings}
          type="number"
          onChange={handleChanges}
        />
        <br />
        <br />
        <button type="submit">Create</button>
      </form>
    </Col>
  );
}