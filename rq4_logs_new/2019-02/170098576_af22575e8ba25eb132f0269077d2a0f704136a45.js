import React, { Component } from "react"
import { Dropdown } from 'react-native-material-dropdown'
import AmountInput from "./../AmountInput"
import {
  View,
  StyleSheet,
  Image
} from "react-native"
import Category from './Category'
class Rest extends Component {
  constructor(props) {
    super(props)
    this.state = {
    }
  }

  reset = () =>{
  
}

  render() {
    return (
      <Category
            ref={'child'}
            name={"Rest groep"}
            progress={this.props.progress}
            duration={500}
            fillColor={"#B43C25"}
            barColor={"#99311d"}
            style={{marginBottom: 30,}}
            clickEvent={this.props.clickEvent}
            dropDownView={null}
            data={null}
            apiUrl={'dairyfishpoultry'}
            reset={this.reset}
            setProgress={this.props.setProgress}
            connection={this.props.connection}
            setConnection={this.props.setConnection}
            min={40}
            max={110}
            scrollTo={this.props.scrollTo}
          >

            <View
              style={{
                flex: 1,
                flexDirection: "row",
                justifyContent: "space-evenly"
              }}
            >
              <Image
                style={{ width: 50, height: 50 }}
                source={require("../../images/burger.png")}
              />
              <Image
                style={{ width: 50, height: 50 }}
                source={require("../../images/cheers.png")}        
              />
               <Image
                style={{ width: 50, height: 50 }}
                source={require("../../images/pizza.png")}        
              />
              <Image
                style={{ width: 50, height: 50 }}
                source={require("../../images/can.png")}
              />
            </View>    
          </Category>
    )
  }
}

const styles = StyleSheet.create({

})
export default Rest