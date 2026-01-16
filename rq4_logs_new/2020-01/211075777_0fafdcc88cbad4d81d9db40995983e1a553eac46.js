import * as WebBrowser from 'expo-web-browser';
import React, {Component} from 'react';
import {
  StyleSheet,View,StatusBar,ImageBackground,Image,Dimensions,Text,ActivityIndicator,KeyboardAvoidingView
} from 'react-native';
import { Ionicons } from '@expo/vector-icons';
import { ScrollView } from 'react-native-gesture-handler';
// export default function AddPhonenumberScreen() {
export default class IndividualServiceScreen extends Component {
     
    constructor(props){
        super(props);
        this.state={

        }; 
    }


render(){

  return (
    //   <View><Text>Individual Service</Text></View>


    <KeyboardAvoidingView  style={styles.container} behavior="padding" enabled>
        <StatusBar barStyle = "dark-content" hidden = {false} backgroundColor = "#00BCD4" translucent = {true}/>
            <View style={styles.mainView}>
                <View style={{flex:3}}>
                    <View>
                        <Image
                        style={{width:'100%',height:'100%',opacity:0.5}}
                        source={require("../assets/images/cleaning.jpg")} 
                        resizeMode="cover"
                        /> 
                    </View>
                    <Ionicons style={styles.customIcon} name="md-arrow-back" size={32} color="green" />
                    <View style={{position:'absolute',bottom:10,justifyContent:'center',flex:1,flexDirection:'row',width:'100%'}}>
                        <Text style={{fontWeight:"bold",letterSpacing:1,fontSize:20,textAlign:'center'}}>Bathroom Deep Cleaning</Text>
                    </View>
                </View>
                <View style={{flex:4}}>
                    <ScrollView>
                        <View style={{padding:5}}>
                            <Text style={{fontSize:16,fontWeight:'bold',letterSpacing:1}}>Bathroom Deep Cleaning covers:</Text>
                        </View>
                        <View style={{flexDirection:'column',justifyContent:'center'}}>
                            <View style={{flexDirection:'row',flex:1,justifyContent:'flex-start'}}>
                                <View style={{height:5,width:5,backgroundColor:'#000',radius:'50%'}}></View>
                                <View></View>
                            </View>
                        </View>
                        
                    </ScrollView>

                </View>
                <View style={{flex:3}}></View>

                {/* For loading purpose only */}
                {/* {this.state.showloading == true ? 
                    <View style={styles.loadingView}>
                        <View style={{flexDirection:'row', justifyContent:"center",width:Dimensions.get('window').width}}>
                            <ActivityIndicator size="large" color="#0000ff" />
                        </View>
                    </View>
                :null
                } */}
                {/* For loading purpose only */}
            </View>
        {/* </ImageBackground> */}
    </KeyboardAvoidingView >
  );
}
}

IndividualServiceScreen.navigationOptions = {
    // title: 'Home',
    header: null,
};


const styles = StyleSheet.create({
    container: { flex: 1,backgroundColor: '#fff'},
    // backgroundImg:{ width: '100%', height: '100%'},
    
    mainView:{ flex:1,flexDirection:"column" },
    customIcon:{position:"absolute",top:0,paddingTop:30,paddingLeft:15},

    // buttonMainView:{ flexDirection:'row', justifyContent:"flex-end",right:10 },
    // customView:{flexDirection:'column'},
    // titleText:{fontSize:25,paddingTop:10,textAlign:"center",fontWeight:"bold", letterSpacing:2},
    // subtitleText:{fontSize:16,paddingTop:15,textAlign:"center",fontWeight:"bold", letterSpacing:1},
    // subtitleTextTwo:{fontSize:11,paddingTop:4,textAlign:"center",color:'#A8A8A8'},
    loadingView: {
        position:"absolute",
        top:Dimensions.get('window').height/2
    }
});