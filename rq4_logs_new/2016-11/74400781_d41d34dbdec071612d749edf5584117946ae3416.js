/**
 * Created by liorf on 11/26/16.
 */
import React from 'react';
import ListOfDishes from '../Dishes/ListOfDishes';

class morningMenu extends React.Component {
  constructor(props) {
    super(props);
    this.componentDidMount = this.componentDidMount.bind(this);
    this.editDish = this.editDish.bind(this);
    this.deleteDish = this.deleteDish.bind(this);

  }

  componentDidMount() {
    console.log('morningMenu | componentDidMount', this.props);
    if (!this.props.restManagerData.appData.data.dishes) {
      this.props.restManagerData.getDishes();
    }
  }

  editDish () {
    console.log('editDish');
  }

  deleteDish () {
    console.log('deleteDish');
  }

  render () {
    console.log('morningMenu | props', this.props.restManagerData.appData);
    var style = {
      display: 'flex',
      flexDirection: 'row',
    };
    var leftStyle = {
      background: 'black',
      width: '70%',
    };
    var rightStyle = {
      background: 'red',
      width: '30%',
    };
    if (!this.props.restManagerData.appData.data.dishes) {
      console.log('loading');
      return (
        <div>loading</div>
      )
    } else {
      return (
        <div>
          {this.props.exit}
          <div name="menuContainer" style={style}>
            <div name="menuLeft" style={leftStyle}>
              morningMenu:
            </div>
            <div name="menuRight" style={rightStyle}>
              Dishes:
              <ListOfDishes dishes={this.props.restManagerData.appData.data.dishes} editDish={this.editDish} deleteDish={this.deleteDish}/>
            </div>
          </div>
        </div>
      )
    }

  }
}

export default morningMenu;