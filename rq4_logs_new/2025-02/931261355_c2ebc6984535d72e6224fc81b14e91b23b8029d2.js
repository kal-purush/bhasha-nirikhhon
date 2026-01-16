import React, { useState, useEffect } from 'react'

function FormMock() {
  const [form, setForm] = useState({
    id: null,
    userId: null,
    title: '',
    completed: false
  });

  useEffect(() => {
    fetch('https://jsonplaceholder.typicode.com/todos/1')
      .then(response => response.json())
      .then(data => {
        setForm(data);
      });

  }, []);

  const handleChange = ({ target }) => {
    const { name, value } = target;
    setForm({ ...form, [name]: value });
  }

  const handleSubmit = (e) => {
    e.preventDefault()
    alert('Form submitted!');
  }

  return (
    <>
    <div className='flex bg-primary flex-col items-start justify-center gap-8 py-10'>
      <h1 className='text-white'>Form Mock</h1>
      <form onSubmit={handleSubmit} className='flex flex-col items-center'>
        <label>Title</label>
        <input name="title" onChange={handleChange} type="text" value={form.title} />
        <p  className=' text-white'>{form.title}</p>
        <label>completed</label>
        <input name="completed" onChange={handleChange} type="checkbox" value={form.completed} />
        <button type="submit">Submit</button>
      </form>
      </div>
    </>
  )
}

export default FormMock