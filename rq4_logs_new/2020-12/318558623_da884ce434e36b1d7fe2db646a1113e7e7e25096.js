import React from "react"


function SessionTiming(props){
    return (
        <section id="control" className="control">
            <div id="break-label" className="break-label">
            <button id="break-decrement" onClick={this.props.breakTimeDecrement}>
                <i className="fa fa-arrow-down fa-2x"></i>
            </button>
            <div id="breakTime" className="breakTime" >{props.breakTime}</div>
            <button id="break-increment" onClick={this.props.breakTimeIncrement}>
                <i className="fa fa-arrow-up fa-2x"></i>
            </button>
            </div>
            <div id="session-label" className="session-label">
                <button id="session-decrement" onClick={this.props.sessionTimeDecrement}>
                <i className="fa fa-arrow-down fa-2x"></i>
                </button>
                <div id="sessionTime" className="sessionTime">{props.sessionTime}</div>
                <button id="session-increment" onClick={this.props.sessionTimeIecrement}>
                <i className="fa fa-arrow-up fa-2x"></i>
                </button>
            </div>
      </section>
    )
}
export default SessionTiming