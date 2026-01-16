import { StatusBar } from 'expo-status-bar';
import { StyleSheet, Text, View, RefreshControl, ScrollView } from 'react-native';
import Figura from './components/figura';
import { useState } from 'react';

let array = new Array(9).fill(0);

export default function App() {
  const [values, setValues] = useState({
    isCross: true,
    winMessage: "",
  });

  const changeMove = (number) => {
    if (array[number] === 0 && !values.winMessage) {
      array[number] = values.isCross;
      setValues({ isCross: !values.isCross });
    }
  }

  return (
    <View style={styles.container}>
      <Text style={styles.text}>Jogo da Velha</Text>
      <View style={styles.row}>
        <View style={styles.box}>
          <Figura vetor={array} posicao={0} clicado={() => changeMove(0)} />
        </View>
        <View style={styles.box}>
          <Figura vetor={array} posicao={1} clicado={() => changeMove(1)} />
        </View>
        <View style={styles.box}>
          <Figura vetor={array} posicao={2} clicado={() => changeMove(2)} />
        </View>
        <View style={styles.box}>
          <Figura vetor={array} posicao={3} clicado={() => changeMove(3)} />
        </View>
        <View style={styles.box}>
          <Figura vetor={array} posicao={4} clicado={() => changeMove(4)} />
        </View>
        <View style={styles.box}>
          <Figura vetor={array} posicao={5} clicado={() => changeMove(5)} />
        </View>
        <View style={styles.box}>
          <Figura vetor={array} posicao={6} clicado={() => changeMove(6)} />
        </View>
        <View style={styles.box}>
          <Figura vetor={array} posicao={7} clicado={() => changeMove(7)} />
        </View>
        <View style={styles.box}>
          <Figura vetor={array} posicao={8} clicado={() => changeMove(8)} />
        </View>
      </View>
      <StatusBar style="auto" />
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: '#fff',
    alignItems: 'center',
    justifyContent: 'center',
  },
  text: {
    fontSize: 40,
    color: '#000',
    fontWeight: 'bold',
    marginBottom: 20,
  },
  row: {
    flexDirection: 'row',
    flexWrap: 'wrap',
    justifyContent: 'center',
  },
  box: {
    borderWidth: 2,
    padding: 10,
    borderColor: '#000',
  },
  winMessage: {
    fontSize: 30,
    color: '#000',
    fontWeight: 'bold',
    marginTop: 20,
  }
});