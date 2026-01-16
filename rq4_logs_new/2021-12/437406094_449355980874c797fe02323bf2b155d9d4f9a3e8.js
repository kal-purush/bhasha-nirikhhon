import React, { Component } from "react";
import DataComponent from "./Components/DataComponent";
//import data from "./data";

class App extends Component {
    constructor(props) {
        super(props);
        this.state = {
        }
    }

    render() {
        return (
            <DataComponent />
        );

           /* 
            <div className="container">
                <div class="jumbotron">
                    <h1 class="display-4">Products</h1>
                </div>
                <div>
                    <input placeholder="Search" />
                </div>
                {data.map((post) => (
                    <div className="card" key={post.id}>
                        <div className="card-header">
                            {post.name}
                        </div>
                        <div className="card-body">
                            <h5>Details: {post.details}</h5>
                            <h5>Price: {post.price}</h5>
                            <h5> Location: {post.location}</h5>
                            <img src="https://via.placeholder.com/150/92c952"></img>
                        </div>
                    </div>
                ))}
            </div>
        );
    }
*/
    }
}
export default App;