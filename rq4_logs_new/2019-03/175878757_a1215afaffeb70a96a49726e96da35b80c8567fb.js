import React from 'react';
import { StyleSheet, Text, View } from 'react-native';

const styles = StyleSheet.create({
  label: { color: '#FFF' },
  tip: {
    color: '#FFF',
    fontSize: 60,
    fontWeight: 'bold'
  },
  total: {
    color: '#FFF',
    fontSize: 30,
    fontWeight: 'bold'
  },
  values: {
    alignItems: 'center',
    backgroundColor: '#484848',
    padding: 20,
    width: '100%'
  }
});

const Values = ({
  bill, tipPercent
}) => {
  let tip = 0.00;
  let total = 0.00;
  if (bill) {
    tip = parseFloat(bill) * tipPercent;
    total = parseFloat(bill) + tip;
    tip = (Math.round(tip * 100) / 100).toFixed(2);
    total = (Math.round(total * 100) / 100).toFixed(2);
  }

  return (
    <View style={ styles.values }>
      <Text style={ styles.label }>
        Tip Amount
      </Text>
      <Text style={ styles.tip }>
        { `$${tip}` }
      </Text>
      <Text style={ styles.label }>
        Total Bill
      </Text>
      <Text style={ styles.total }>
        { `$${total}` }
      </Text>
    </View>
  );
};

export default Values;