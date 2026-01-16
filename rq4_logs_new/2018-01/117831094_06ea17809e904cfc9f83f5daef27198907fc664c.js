import React from 'react';
import {StyleSheet, View} from 'react-native';
import PeopleList from './list/PeopleList';


export default class App extends React.Component {
    render() {
        return (
            <View style={styles.container}>
                <PeopleList />
            </View>
        );
    }
}



const styles = StyleSheet.create({
    container: {
        backgroundColor: '#f5f5f5',
    },
});