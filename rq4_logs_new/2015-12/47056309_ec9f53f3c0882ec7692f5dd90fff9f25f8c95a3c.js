import React from "react";

export default class AppNavBar extends React.Component {
    render() {
        return (
            <nav className="navbar navbar-default">
                <div className="container-fluid">
                    <div className="navbar-header">
                        <a className="navbar-brand" href="#">3PS</a>
                    </div>
                    <div className="collapse navbar-collapse" id="bs-example-navbar-collapse-1">
                        <ul className="nav navbar-nav">
                            <li className="active"><a href="#">Summary <span className="sr-only">(current)</span></a></li>
                            <li><a href="#">History</a></li>
                            <li><a href="#">Admin</a></li>
                        </ul>
                    </div>
                </div>
            </nav>
        )
    }
}