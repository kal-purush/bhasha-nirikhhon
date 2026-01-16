import { StatusBar } from 'expo-status-bar';
import { StyleSheet, Text, View,Button } from 'react-native';

export default function App() {
  const message="javacript";

  return (
    <View style={styles.container}>
      <Text style={styles.title} >react native</Text>
      <Text style={styles.content} >react js</Text>
      <Text style={styles.content} >{message}</Text>
      <Button   title="Learn More"  color="#841584" 
       accessibilityLabel="Learn more about this purple button"/>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: '#fff',

    alignItems: 'center',
    justifyContent: 'center',
  },
  title: {
    fontSize: 100,

  },
  content:{
    fontSize:20,
    color:"blue",
    backgroundColor:"yellow"
  }
});