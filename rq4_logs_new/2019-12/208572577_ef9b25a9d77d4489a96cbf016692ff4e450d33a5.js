import React from 'react';
import { connect } from 'react-redux';
import { searchShowAction } from '../../actions/searchShowAction';
import { Grid, Container, Input, Button } from '@material-ui/core';
import SearchSharpIcon from '@material-ui/icons/SearchSharp';

import './Search.scss';

class Search extends React.Component {

  constructor(props) {
    super(props);
    this.state = {
      query: ''
    };
  }

  handleChange = (event) => {
    this.setState({ query: event.target.value });
  }

  handleClick = () => {
    this.props.dispatch(searchShowAction(this.state.query));
  }

  render() {
    return (
      <Container className="root">
        <Grid container direction="row" spacing={3} className="grid-container">
          <Grid item xs={9} className="input-grid">
            <Input
              fullWidth={true}
              value={this.state.query}
              onChange={this.handleChange.bind(this)}
              placeholder='Start by tying show name...'
              autoFocus={true}
            >
            </Input>
          </Grid>  
          <Grid item xs={2} className="btn-grid">            
            <Button 
              onClick={this.handleClick.bind(this)} 
              disableRipple={true}              
            >
                <SearchSharpIcon style={{fontSize: 26, color: "white", margin: "0 5px 0 -3px"}}></SearchSharpIcon>
                Search
            </Button>
          </Grid>
        </Grid>
      </Container>
    );
  }
}

export default connect()(Search);