import React from 'react';
import ProgressBar from 'react-bootstrap/ProgressBar';
import './index.css';

class App extends React.Component {

  dropHandler = (e) => {
    e.preventDefault();
    console.log("Files dropped");
    
  }

  dragOverHandler = (e) => {
    e.preventDefault();
    console.log('dragOverHandler');
  }

  render() {
    return (
      <div className="container">
          
          <ProgressBar variant="danger" now={100} />

          <div id="bot">
            <div id="drop_zone" onDrop={this.dropHandler} onDragOver={this.dragOverHandler} >
              <p>Drag files here or Browse</p>
              <input type="file" id="file" />

            </div>
          </div>
      </div>
    );
  }
}

export default App;