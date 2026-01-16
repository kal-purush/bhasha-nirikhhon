import React,{Component} from 'react';
import {
  View,
  StyleSheet,
  Text,
  Image,
  TouchableOpacity
}from 'react-native';


export default class PersonDetailVC extends React.Component {
  constructor(props) {
    super(props);
  }

  _returnAction = ()=>{
    const { navigator } = this.props;
    if (navigator) {
      navigator.pop();
    }
  }

  render(){
    return(
      <View style = {styles.container}>
        <Text>人员编号:{this.props.person.id}</Text>
        <Text>人员姓名:{this.props.person.name}</Text>
        <Text>人员路径:{this.props.person.source}</Text>
        <TouchableOpacity onPress={this._returnAction} style = {styles.button}><Text style = {{color:'white'}}>点击我进行返回</Text></TouchableOpacity>
      </View>
    )
  }
}

const styles = StyleSheet.create({
  container:{
    flex : 1,
    flexDirection : "column",
    backgroundColor: "white",
    marginTop:64,
    alignItems:'center'
  },
  button:{
    marginTop:64,
    height:20,
    width:100,
    backgroundColor:'darkslateblue'
  }
});