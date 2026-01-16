import React, { Component } from 'react';
import Assets from './components/Assets'
import Auctions from './components/Auctions'
import './App.css';
import { Proxy } from 'braid-client'
import CircularProgress from '@material-ui/core/CircularProgress'
import { Grid, Paper } from '@material-ui/core';

class App extends Component {
  constructor() {
    super()

    this.state = {
      loaded: false
    }

    this.onOpen = this.onOpen.bind(this)
  }
  componentDidMount() {
    this.braid = new Proxy({
      url: "http://localhost:8085/api/"
    }, this.onOpen, this.onClose, this.onError, { strictSSL: false });
  }

  render() { 
    let body
    
    if (this.state.loaded) {
      body = (
        <div>
          <h1>Cordutch</h1>
          <Grid container spacing={3}>
            <Grid item xs={12}>
              <Paper>
                <h2>Assets</h2>
                <Assets braid={this.braid} />
              </Paper>
            </Grid>

            <Grid item xs={12}>
              <Paper>
                <h2>Auctions</h2>
                <Auctions braid={this.braid} />
              </Paper>
            </Grid>
          </Grid>
        </div>
      )
    } else {
      body = (
        <CircularProgress />
      )
    }


    return (
    <div>
      {body}
    </div>

  );}

  onOpen() {
    console.log('Connected to node.')
    this.setState({loaded: true})
  }
  onClose() { console.log('Disconnected from node.') }
  onError(err) { console.error(err); }
}

export default App;