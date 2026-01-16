import './style/main.scss';

import React from 'react';
import ReactDom from 'react-dom';
import superagent from 'superagent';



class SearchForm extends React.Component {
  constructor(props) {
    super(props)
    this.state = {
      val: '',
      number: 0,
    };

    this.handleChange = this.handleChange.bind(this);
    this.handleSubmit = this.handleSubmit.bind(this);
  }

  handleChange(e) {
   
    if(e.target.name === 'Reddit-Board') this.setState({val: e.target.value});
    if(e.target.name === 'number') this.setState({number: e.target.value});
  }

  handleSubmit(e) {
    e.preventDefault();
    // console.log(this.props.get_set_app)
    this.props.update_state(this.state.val, this.state.number);
  }

  render() {
    var disabled = this.props.error;
    var required = this.props.reddit;
    
    return (
      
      <form
      className="search-form"
      onSubmit={this.handleSubmit}
      disabled={disabled} required={required}
      >
      
        <h1>What do you want to search Reddit for?</h1>

        <input
          type="text"
          name="Reddit-Board"
          value={this.state.val}
          onChange={this.handleChange}
          placeholder="Enter some text here"/>

          <input
          id="limit"
          type="text"
          name="number"
          value={this.state.number}
          onChange={this.handleChange}
          placeholder="0"/>

        <button type="submit">Search</button>

       
      </form>
    )
  };
};


class SearchResultsList extends React.Component {
  constructor(props) {
    super(props)
   
  }

  
 
  render() {
    return (
      <div className="results">
        {this.props.reddit ?  
          <section className="reddit-data">
            
            <h2>Sub-Reddit: {this.props.reddit.data.children[0].data.subreddit}</h2>
            <ul>
              {this.props.reddit.data.children.map((x, i) => {
                return (<li name={x.data.title} key={i} >
                <a href={x.data.url}>
                <h4>{x.data.title}</h4>
                <p className="thumbs">Thumbs up:  {x.data.ups}</p>
                </a>
                </li>)
              })}
            </ul>

          
          </section>
          :
          undefined
        }

        {this.props.error ?
          <section className="reddit-error">
            <h2>You broke it.</h2>
          </section>
          :
          undefined
        }
      </div>
    )
  }
}




class App extends React.Component {
  constructor(props) {
    super(props)
    this.state = {
      reddit: null,
      searchError: null,
    }
    this.searchApi = this.searchApi.bind(this);
    this.updateState = this.updateState.bind(this);
  }

  updateState(name, number) {
    this.searchApi(name, number)
    .then(res => this.setState({reddit: res.body, searchError: null}))
    .catch(err => {
      this.setState({reddit: null, searchError: err});

    })
  }

  searchApi(name, number) {
    console.log('name in searchApi', name)
    name = name.replace(/\s/g,'');
    return superagent.get(`https://www.reddit.com/r/${name}.json\?limit\=${number}`);
  }
 
  render() {
    return (
      <div className="application">
        <SearchForm update_state={this.updateState}/>
        <SearchResultsList reddit={this.state.reddit} error={this.state.searchError}/>
      </div>
    )
  }
}

ReactDom.render(<App />, document.getElementById('root'));