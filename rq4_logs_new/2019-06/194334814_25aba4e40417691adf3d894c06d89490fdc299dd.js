import React, { Component } from 'react';

class ApprovalCard extends Component {
    render(){
        return(
            <div className="ui card">
                <div classname='content'>
                    Are you working?
                </div>
                <div className='extra content'>
                    <div className="ui two buttons">
                        <div className="basic ui green button">Approve</div>
                        <div className="basic ui red button">Reject</div>

                    </div>
                </div>


            </div>
        )
    }
}

export default ApprovalCard;