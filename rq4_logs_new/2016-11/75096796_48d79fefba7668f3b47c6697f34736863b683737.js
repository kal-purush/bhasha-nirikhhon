import React, { Component } from 'react';
import './App.css';

import Grocery from './Grocery'

// const groceries = [
//   { ref: 1,
//     name: 'bananas',
//     quantity: '17 bunches',
//     notes: 'this is a banana note',
//     purchased: false,
//     starred: false
//   },
//   { ref: 2,
//     name: 'apples',
//     quantity: '45 of them',
//     notes: 'this is an apple note',
//     purchased: false,
//     starred: false
//   },
// ]

class App extends Component {
  constructor() {
    super();
    this.state = {
      groceries: [
        { ref: 1,
          name: 'bananas',
          quantity: '17 bunches',
          notes: 'this is a banana note',
          purchased: false,
          starred: false
        },
        { ref: 2,
          name: 'apples',
          quantity: '45 of them',
          notes: 'this is an apple note',
          purchased: false,
          starred: false
        },
      ]
    };
  }
  onClearGroceries(){

  }
  render() {
    return (
      <div>
        <button className='clear-groceries' disabled={this.state.groceries.length === 0} onClick={()=>this.onClearGroceries()}>Clear Groceries</button>
        {this.state.groceries.map(item =>
          <Grocery
            key={item.ref}
            name={item.name}
            quantity={item.quantity}
            purchasd={item.purchased}
            starred={item.starred}
          />
        )}
      </div>
    );
  }
}

export default App;