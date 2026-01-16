import React,{useEffect, useState} from 'react'

import './App.css'

const App = () => {
  const [data,setData] = useState([]);
  const [search, setSearch] = useState('');
  useEffect(() => {
    fetch('https://jsonplaceholder.typicode.com/todos/').then(
      response => response.json()
    ).then(jsondata => setData(jsondata))
  },[search])
  const onSubmit = e =>{
    e.preventDefault();
    setSearch(search);
  }
  return (
    <div>
      <center>
        <h1 className="main-heading">USERS LIST</h1>
        <div className="container-fluid"  m-6="true">
            <form className="d-flex" onSubmit={onSubmit} >
              <input className="form-control me-2" type="text" onChange={(e) => setSearch(e.target.value)} placeholder="Search" aria-label="Search" />

            </form>
        </div>
        <table className="table"> 
          <thead className="thead">
              <tr>
                <th className="heading">user-Id</th>
                <th className="heading">ID</th>
                <th className="heading">TITLE</th>
                <th className="heading">COMPLETED</th>
              </tr>
          </thead>
          <tbody>
            {data.filter(item => item.title.toLowerCase().includes(search.toLowerCase())).map(item => {
              return (
                <tr>
                  <td >{item.userId}</td>
                  <td >{item.id}</td>
                  <td >{item.title}</td>
                  <td>{item.completed}</td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </center>
    </div>
  )
}

export default App