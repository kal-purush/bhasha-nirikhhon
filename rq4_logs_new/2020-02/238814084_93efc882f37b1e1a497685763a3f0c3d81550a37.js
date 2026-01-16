import React, { Component } from 'react';
import SubscribeInput from '../../molecules/SubscribeInput';
class Footer extends Component {
  render() {
    return (
        <footer className="page-footer font-small bg-dark pt-4">
            <SubscribeInput></SubscribeInput>
            <div className="container-fluid text-center text-md-left text-white">
                <div className="footer-copyright text-center py-3">
                    © 2020 Copyright
                </div>
            </div>
        </footer>
        
    );
  }
}

export default Footer