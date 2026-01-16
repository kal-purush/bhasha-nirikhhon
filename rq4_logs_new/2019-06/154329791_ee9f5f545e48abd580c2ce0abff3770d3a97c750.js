import React from 'react';
import './Contact.scss'
import 'bootstrap/dist/css/bootstrap.css';
import PropTypes from 'prop-types';
import { withStyles } from '@material-ui/core/styles';
import { library } from '@fortawesome/fontawesome-svg-core';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { far, faEnvelopeOpen } from '@fortawesome/free-regular-svg-icons';
import { fab, faLinkedinIn } from '@fortawesome/free-brands-svg-icons';

library.add(far, fab, faEnvelopeOpen , faLinkedinIn)

class Contact extends React.Component {
    constructor(props) {
      super(props);
    }

    linkedIn(){
      window.open('https://www.linkedin.com/in/maria-ochoa-44196b138/')
    }

    render() {
      return (
        <div className="container-fluid m-0 p-0 w-100 contact" id='Contact'>
          <div className="row h-100 m-0 p-0">
            <div className="col h-100 m-0 p-0">
              <div className='container-fluid w-100 m-0 p-0 titleContainer'>
              <div className='row m-0 p-0 w-100 d-flex justify-content-center'>
               <div className='col w-100'><h1 className='title'>Contact Me!</h1></div>
               </div>
               </div>
               <div className='container-fluid w-100 m-0 p-0 iconContainer'>
               <div className='row m-0 p-0 w-100 d-flex justify-content-center align-content-center iconContainer'>
                 <div className='col-auto p-0'><a href='mailto:mariaviiia@hotmail.com'><FontAwesomeIcon icon={['far','envelope-open']} className='iconEmail'/></a></div>
                 <div className='col-auto p-0' onClick={this.linkedIn}><FontAwesomeIcon icon={['fab','linkedin-in']} className='iconLinked'/></div>
               </div>
               </div>
              </div>
            </div>
          </div>
      );
    }
  }

  export default (Contact)