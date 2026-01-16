import React, { Component } from 'react';
import '../App.css';
import Nav from './Nav'
import hogs from '../porkers_data';
import FilterContainer from './FilterContainer'
import HogContainer from './HogContainer'

class App extends Component {

  state = {
    // hogs: [],
    sortValue: 'All',
    filterValue: false
  }

  handleSort = (newSortValue) => {
    // console.log(newSortValue)
    this.setState({
      sortValue: newSortValue
    })
  }

  handleFilter = () => {
    this.setState(prevState => {
      return {
        filterValue: !prevState.filterValue
      }
    })
  }

  sortHogs = () => {
    let {sortValue} = this.state

    if (sortValue === "Name") {
      return [...hogs].sort((hogA, hogB) => {
        return hogA.name.localeCompare(hogB.name)
      })
    } else if (sortValue === "Weight"){
      return [...hogs].sort((hogA, hogB) => {
        return hogA.weight - hogB.weight
        
      })
    // } else if (sortValue === "Weight"){
    //   return [...hogs].sort((hogA, hogB) => {
    //     if (hogA.weight > hogB.weight) {
    //       return 1
    //     } else if (hogA.weight < hogB.weight) {
    //       return -1
    //     } else {
    //       return 0
    //     }
    //   })
    } else {
      return [...hogs]
    }
  }

  applyFilter = (finalHogs) => {
    let {filterValue} = this.state
    if (filterValue){
      return [...finalHogs].filter(hog => hog.greased)
    } else {
      return [...finalHogs]
    }
  }

  render() {
    // console.log(this.state)
    return (
      <div className="App">
          < Nav />
          < FilterContainer handleSort={this.handleSort} handleFilter={this.handleFilter}/>
          < HogContainer  hogsArr={this.applyFilter(this.sortHogs())}/>
      </div>
    )
  }
}

export default App;
// hogsArr={hogs}