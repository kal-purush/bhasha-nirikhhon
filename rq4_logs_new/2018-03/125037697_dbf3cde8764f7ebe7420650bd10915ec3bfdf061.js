import React, { Component } from 'react';
import {
    View,
    Text,
    StyleSheet,
    ScrollView,
    TextInput,
    Button,
    FlatList} from 'react-native';

export default class home extends Component{
    static navigationOptions= ({navigation}) =>({
        title: 'Welcome',
    });
    constructor (props) {
        super(props)
        this.state = {
            messagesArray: [],
            text:''
        }
    }

    onMessageSend () {
        if(this.state.text) {
            let messages = this.state.messagesArray
            messages.push({
                'text': this.state.text
            })
            this.setState ({
                messagesArray: messages,
                text: ''
            })
        }
    }

    render(){
        const {navigate} = this.props.navigation;
        return(
            <View style={styles.block}>
                <View style={styles.container}>
                    <Text style={styles.pageName}>Welcome to the public chat room!</Text>
                </View>
                <ScrollView>
                    {this.state.messagesArray.map((mes, index) => {
                        return (
                            <View>
                                <Text style={styles.text}>{mes.text}</Text>
                            </View>
                        )
                    })}
                </ScrollView>
                <View>
                    <TextInput
                        style={styles.textInput}
                        value={this.state.text}
                        onChangeText={(text) => this.setState({text})}/>
                    <Button style={styles.message} title='Send Message' onPress={() => this.onMessageSend()}/>
                </View>

            </View>

        );
    }
}
const styles = StyleSheet.create({
    block: {
        flex: 1
    },
    text: {
        fontWeight:'bold',
        fontSize: 15
    },
    container: {
        display:'flex',
        alignItems:'center',
        justifyContent:'center'
    },
    pageName: {
        margin: 10,
        fontWeight:'bold',
        color:'#000',
        textAlign:'center',
        fontSize: 20
    },
    message: {
        flex: 2,
        flexDirection: 'column',
        justifyContent: 'flex-end',
        alignItems: 'center',
        margin: 5,
        bottom: 0,
        right: 0,
        left: 0,
    }
});