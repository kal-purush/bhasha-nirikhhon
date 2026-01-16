import React, { Component } from 'react';
import { Text, View} from 'react-native';

export default class Agenda extends Component {

    constructor(props) {
        super(props);
    }

    render() {
        return(
            <View><Text>This is the Agenda component</Text></View>
        );
    }
}