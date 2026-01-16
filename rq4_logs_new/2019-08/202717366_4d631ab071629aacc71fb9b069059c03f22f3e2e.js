import React, { Component } from 'react';
import './App.css';
import { Link } from "react-router-dom";



class App extends Component {
  constructor(props){
    super(props);

    this.state = {
      posts: [],
    }

    this.headers = [
      { key: "userId", label: "USERID"  },
      { key: "id", label: "ID"  },
      { key: "title", label: "Title"  },
      { key: "body", label: "Body"  }
    ];

    this.fetchPostStories = this.fetchPostStories.bind(this);

  }

  fetchPostStories(result){
    this.setState({ posts: result})
  }

  componentDidMount(){
    fetch(`https://jsonplaceholder.typicode.com/posts`)
    .then(response => response.json())
    .then(result => this.fetchPostStories(result) )
  }


  render(){
    console.log(this.state.posts)
    return(
      <div className="App">
        <header className="App-header">
          
          <p>
            Fetch APi
          </p>
         
        </header>

        <section>
            <table>
              <thead>
                <tr>
                  {
                   this.headers.map(item => (
                      <th key={item.key}>{item.label}</th>
                    ))
                  }
                </tr>
              </thead>

              <tbody>
               
                  {
                    this.state.posts.map((item, key) =>(
                        <tr key={key}>
                          <td>{item.userId}</td>
                          <td>{item.id}</td>
                          <td>
                          <Link to={`/posts/${item.id}`}>{ item.title }</Link>
                          </td>
                          <td>{item.body}</td>
                        </tr>
                      ))
                  }
           
              </tbody>

            </table>
        </section>
    </div>
    )
  }
}

export default App;