import React, { useState, } from 'react';
import { SafeAreaView, View, Text, TouchableOpacity, TextInput, StyleSheet, Button } from 'react-native';
import api from './src/services/api';

export default function App() {

  const [pegaCep, setPegaCep] = useState({});
  const [searchCep, setSearchCep] = useState('');

  const getCep = async () => {
    const { data } = fetch(`/${searchCep}/json/`)
    setPegaCep(data);

  }

  return (
    <SafeAreaView style={styles.container}>
      <View>
        <View>
          <TextInput
            style={styles.spaceInput}
            onChangeText={text => setSearchCep(text)}
            value={searchCep}
            placeholder='Digite o cep aqui'
          />
          <Button
            title='Buscar'
            onPress={getCep}
          />
        </View>
        <View >
          <View style={styles.blocView}>
            <Text>Rua:{pegaCep.logradouro}</Text>
          </View>
          <View style={styles.blocView}>
            <Text>Bairro:{pegaCep.bairro}</Text>
          </View>
          <View style={styles.blocView}>
            <Text>Cidade:{pegaCep.localidade}</Text>
          </View>
          <View style={styles.blocView}>
            <Text>Estado:{pegaCep.uf}</Text>
          </View>
        </View>
      </View>
    </SafeAreaView>
  )
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    justifyContent: 'center',
    alignItems: 'center',
  },
  blocView: {
    padding: 10,
    backgroundColor: '#dddd',
    marginTop: 10,
    borderRadius: 20,
  },
  spaceInput:{
    backgroundColor: '#dada',
    borderRadius: 10,
    padding: 5,
    marginBottom: 10,
  }
})












