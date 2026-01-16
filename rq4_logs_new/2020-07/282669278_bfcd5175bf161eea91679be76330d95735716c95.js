import React, { Fragment } from 'react'
import { Text, View, StyleSheet, TouchableHighlight } from 'react-native'

const OrderButton = (props) => {
  const { dataSource } = props;
  return (
    <Fragment>
      <TouchableHighlight style={styles.button} underlayColor="black">
        <View style={styles.row}>
          <View style={styles.items}>
            <Text style={styles.buttonText}>{`${dataSource.qty} ${dataSource.product}`}</Text>
          </View>
        </View>
      </TouchableHighlight>
    </Fragment>
  )
}

const styles = StyleSheet.create({
  button: {
    margin: 10,
    padding: 10,
    borderWidth: 2,
    borderRadius: 10,
    alignSelf: 'stretch'
  },
  buttonText: {
    fontSize: 26,
    textAlign: 'center'
  },
  row: {
    flexDirection: 'row',
    alignItems: 'center'
  },
  items: {
    height: 'auto',
    width: 'auto',
    margin: 5,
    borderRadius: 10
  }
});

export default OrderButton