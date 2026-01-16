import React, { useState, useEffect, Component } from "react";
import { View, Text, TextInput, StyleSheet, SafeAreaView, FlatList} from 'react-native';

export default class Display extends Component {
    state = {
        data: []
    }

    componentDidMount() {
        this.fetchData();
    }

    fetchData = async () => {
        const response = await fetch('https://api.jsonbin.io/b/61cd9e96c912fe67c50b7287');
        const json = await response.json();
        this.setState({ data: json });
    }

    render() { return (
        <View>
            
            <FlatList
                data={this.state.data}
                keyExtractor={(x, i) => i}
                renderItem={({ item }) =>
                <Text>
                    {`${item.SYMBOL}`}
                </Text>}
            />
            
        </View>
    )
};
}