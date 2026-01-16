import React from 'react';

//Components
import TableBody from './TableBody';

//Libraries
import { format } from 'timeago.js';

class App extends React.Component {
  constructor() {
    super();
    this.state = {
      data: []
    }
  }

  componentDidMount() {
    setInterval(async () => {
      const res = await fetch('http://openlibrary.org/recentchanges.json?limit=10');
      const data = await res.json();
      const formatData = this.formatData(data);
      this.setState({ data: formatData });
    }, 1000);
  }

  formatData(data) {
    return data.map((data, index) => {
      return {
        'when': format(data.timestamp),
        'who': data.author.key,
        'description': data.comment
      };
    });
  }

  render() {
    return (
      <div className="container has-text-centered">
        <h1 className="title is-2">{this.props.title}</h1>

        <table className="table is-bordered is-striped is-narrow is-hoverable is-fullwidth">
          <thead>
            <tr className="has-background-success">
              {
                this.props.headings.map((heading, index) => <th key={index} className="has-text-white">{heading}</th>)
              }
            </tr>
          </thead>  
          <tbody>
            {
              this.state.data.map((data, index) => {
                return <TableBody key={index} data={data} />
              })
            }
          </tbody>
        </table>
      </div>
    ); 
  }
}

export default App;