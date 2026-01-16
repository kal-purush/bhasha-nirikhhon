import React, { PureComponent } from 'react'
import {
  View,
  FlatList,
  TextInput,
  Button,
} from 'react-native'

export default class extends PureComponent {
  constructor() {
    super()
    this.state = {
      show: true,
      data: new Array(200).fill().map((_, i) => ({
      id: i,
      })),
    }
  }

  render() {
    return (
      <View style={{ paddingTop: 20 }}>
        <Button
          onPress={() => { this.setState({ show: !this.state.show })}}
          title="Press"
          color="#841584"
          accessibilityLabel="Learn more about this purple button"
        />
        <FlatList
          data={this.state.show ? this.state.data : []}
          keyExtractor={({ id }) => id}
          renderItem={({ item }) => (
            <View>
              <TextInput
                placeholder="TextInput"
              />
            </View>
          )}
        />
      </View>
    )
  }
}