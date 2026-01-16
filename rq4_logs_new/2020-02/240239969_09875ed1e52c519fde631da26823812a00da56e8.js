import React, { Component } from "react";
import {FormBook} from './FormBook'
export class Book extends Component {
  constructor(props) {
    super(props);
    this.state = {};
  }
  onBookAdded(data){
    console.log(data)
    this.props.onBook(data)
  }

  render() {
    return (
      <div className="container">
        <div className="card mt-5">

        <FormBook onBookAdded={(e)=>{this.onBookAdded(e)}}/>

          {/* <img src="" className="card-img-top"/> 
           
            <div className="card-body">
          <h5 className="card-title">Card title</h5>
          <p className="card-text">Some quick example text to build on the card title and make up the bulk of the card's content.</p>
          <a  className="btn btn-primary">Go somewhere</a>
            </div> */}

        </div>
      </div>
    );
  }
}