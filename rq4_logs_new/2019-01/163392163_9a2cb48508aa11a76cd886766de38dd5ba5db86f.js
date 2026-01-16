import React, { Component } from 'react';
import { StyleSheet, Platform, Image, Text, View, FlatList } from 'react-native';
import firebase from 'firebase';
import axios from 'axios';
import Header from './Header';
import User from './User';
import JobInfo from './JobInfo';

export default class Home extends Component {

   

    state = { listJob: [] }


    componentWillMount() {
        axios.get('https://jobs.github.com/positions.json')
            .then(response => this.setState({ listJob: response.data }))
    }

    _renderItem({ item }) {
        return (
            <JobInfo
                image={item.company_logo}
                text={item.company}
            />
        );
    }

    render() {
        return (
            <View style={styles.viewing}>
                <FlatList
                    horizontal={false}
                    data={this.state.listJob}
                    renderItem={this._renderItem.bind(this)}
                    keyExtractor={item => item.id}
                />
            </View>
        );
    }

}

const styles = StyleSheet.create({
    viewing: {
        position: "absolute",
        top: 0,
        right: 0,
        left: 0,
        bottom: 0,
        flexDirection: 'column',
        alignItems: 'stretch',
        flex: 1
    }
})