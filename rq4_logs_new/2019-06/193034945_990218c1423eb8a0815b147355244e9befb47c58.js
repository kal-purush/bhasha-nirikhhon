import React from 'react';
import { StyleSheet, Text, View, Image, TouchableOpacity, Alert } from 'react-native';
//import ButtonName from "./src/components/ButtonName"

export default class App extends React.Component {

  constructor() {
    super();
    this.state = { rand: require("./src/images/dice1.png"),
    rand2: require("./src/images/dice1.png") }
  }
  getRandom1 = () => {
    return Math.floor(Math.random() * 6) + 1

  }
  getRandom2 = () => {
    return Math.floor(Math.random() * 6) + 1

  }


  btnPress = () => {
   // Alert.alert("Hi"+ this.getRandom())
   var rNum = this.getRandom1()
   var lNum = this.getRandom2()
   switch (rNum) {
     case 1:
       this.setState({ rand: require("./src/images/dice1.png") })
       break;
      case 2:
        this.setState({rand:require("./src/images/dice2.png")})
        break;
      case 3:
        this.setState({rand:require("./src/images/dice3.png")})
        break;
      case 4:
        this.setState({rand:require("./src/images/dice4.png")})
        break;
      case 5:
        this.setState({rand:require("./src/images/dice5.png")})
        break;
      case 6:
        this.setState({rand:require("./src/images/dice6.png")})
        break;
     default:
       break;
   }
   switch (lNum) {
    case 1:
      this.setState({ rand2: require("./src/images/dice1.png") })
      break;
     case 2:
       this.setState({rand2:require("./src/images/dice2.png")})
       break;
     case 3:
       this.setState({rand2:require("./src/images/dice3.png")})
       break;
     case 4:
       this.setState({rand2:require("./src/images/dice4.png")})
       break;
     case 5:
       this.setState({rand2:require("./src/images/dice5.png")})
       break;
     case 6:
       this.setState({rand2:require("./src/images/dice6.png")})
       break;
    default:
      break;
  }
  }
  render() {


    return (
      <View style={styles.container}
      >
        <Image style = {{flex:1,  marginTop:30,} }
        source={this.state.rand}></Image>
        <Image style = {{flex:1}} source={this.state.rand2}></Image>
        <TouchableOpacity>
          <Text onPress={this.btnPress}
          style={styles.btnstyle}
            >Roll Dice</Text>
          
        </TouchableOpacity>

      </View>
    );
  }
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: '#D63031',
    alignItems: 'center',
    justifyContent: 'center',
  },
  btnstyle: {
    marginTop: 30,
    fontSize: 22,
    fontWeight:"bold",
    backgroundColor:"#fff",
    color: "#000",
    borderWidth: 2,
    paddingHorizontal: 40,
    paddingVertical: 15,
    borderRadius: 10,
    marginBottom:20
  }
});