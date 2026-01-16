import React from 'react';
import HelloWorld from './HelloWorldApp.js';
import { StyleSheet, Text, View } from 'react-native';
import Camera from './scanner'

export default class App extends React.Component {
  constructor(props) {
    super(props)
    this.state = {
      qrcode: ''
    }
  }
  render() {
    return (
      <View style={styles.container}>
        <HelloWorld />
        {/* // <View style={styles.container}>

        //   <Text>Identity App</Text>
        //   <Text>Open up App.js to start working on your app!</Text>
        //   <Text>Changes you make will automatically reload.</Text>
        //   <Text>Shake your phone to open the developer menu.</Text>
        //   <View style={styles.comntainer}>
        //    <HelloWorld />
        //    </View>

        // </View> */}
        
        <Camera
          style={styles.preview}
          onBarCodeRead={(e) => alert(e.data)}
          ref={cam => this.camera = cam}
          aspect={Camera.constants.Aspect.fill}
         >
         <Text style={{
           backgroundColor: 'white'
           }}>{this.state.qrcode}</Text>
         </Camera>
      </View>
    );
  }
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: '#fff',
    alignItems: 'center',
    justifyContent: 'center',
  },
});