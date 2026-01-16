import React from 'react';
import {
  Text,
  View,
  FlatList
} from 'react-native';

import styles from './styles';

const axios = require('axios');

export default class DogList extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      doggos: {},
    };
  }

  componentDidMount() {
    axios
      .get('https://dog.ceo/api/breeds/list/all')
      .then(response => {
        this.setState({
          doggos: response.data.message
        });
      })
      .catch(err => {
        console.log(err);
      });
  }

  

  render() {
    return (
      <View style={styles.container}>
        <Text style={styles.header}>Doggos</Text>
        <FlatList
          data={Object.keys(this.state.doggos)}
          renderItem={({item}) =>
            <View style={styles.list}>
                <Text key={item} style={styles.text}>
                  {item}
                </Text>
            </View>
          }
          keyExtractor={(item, index) => item+index}
        />
      </View>
    );
  }
}