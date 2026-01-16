import React, { Component } from 'react';
import { Text, View, StyleSheet, TextInput, ScrollView } from 'react-native'

class Greeting extends Component {
    render() {
        return (
          <Text>Hello {this.props.name}</Text>
        );
    }
}

class Blink extends Component {
    constructor(props) {
        super(props);
        this.state = {
            showText: true
        }
        setInterval(() =>{
            this.setState(previousState => {
                return {
                    showText: !previousState.showText
                }
            })
        },1000)
    }
    render() {
        return (
            <Text style={this.props.style}>{this.state.showText ? this.props.text : ' '}</Text>
        );
    }
}

class PizzaTranslator extends Component {
    constructor(props){
        super(props);
        this.state = {
            text: ''
        }
    }

    render() {
        return (
            <View style={{padding: 10,width: '80%'}}>
                <TextInput
                    style={{height: 40}}
                    placeholder="type here to translate" 
                    onChangeText={(text)=>this.setState({text})}
                />
                <Text style={{fontSize: 24}}>{this.state.text.split(' ').map((word)=>word && '🍕').join('')}</Text>
            </View>
        );
    }
}

class IScrolledDownAndWhatHappenedNextShockedMe extends Component {
    render() {
        
    }
}

export default class LotsOfGreetings extends Component {
    render() {
        return (
          <View style={{alignItems: 'center'}}>
              <PizzaTranslator/>
          </View>
        );
    }
}

const styles = StyleSheet.create({
  red:{
      color: 'red'
  },
  bold: {
      fontWeight: 'bold'
  },
  bigBlue: {
      color: 'blue',
      fontWeight: 'bold',
      fontSize: 30
  }
})