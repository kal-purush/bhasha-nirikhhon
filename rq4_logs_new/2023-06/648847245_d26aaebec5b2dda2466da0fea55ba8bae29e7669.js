import React, { useState } from 'react';
import { StyleSheet, Text, View, SafeAreaView, TextInput } from 'react-native';
import ButtonWithLoader from '../components/ButtonWithLoader';
import Table from '../components/Table';
import SliderComponent from '../components/slider';

const Screen1 = () => {
  const [waitingForSetState, setWaitingForSetState] = useState(false);
  const [waitingForSetIntensity, setWaitingForSetIntensity] = useState(false);
  const [waitingForSetMorse, setWaitingForSetMorse] = useState(false);
  const [intensity, setIntensity] = useState('0');
  const [state, setState] = useState('off');
  const [morse, setMorse] = useState('');
  const [historyArrObject, setHistoryArrObject] = useState('[]')
  const [waitingForGetState, setWaitingForGetState] = useState(false);
  const [stateGot, setStateGot] = useState('Nenhum');

  const onPressSetState = async () => {
    setWaitingForSetState(true);
    let newState = state == 'on' ? 'off' : 'on';
    fetch(`http://localhost:80/setState/${newState}`)
      .then(response => response.text())
      .then(data => {
        setState(newState);
        console.log(data);
        alert(data);
      })
      .catch(error => {
        console.error('Erro:', error);
        alert(error);
      })
      .finally(() => {
        setWaitingForSetState(false);
      });
  };

  const onPressSetIntensityLed = () => {
    setWaitingForSetIntensity(true);
    let newIntensity = intensity == '' ? -1 : intensity; 
    fetch(`http://localhost:80/intensity/${newIntensity }`)
      .then(response => response.text())
      .then(data => {
        console.log(data);
        alert(data);
      })
      .catch(error => {
        console.error('Erro:', error);
        alert(error);
      })
      .finally(() => {
        setWaitingForSetIntensity(false);
      });
  };

  const onPressSetMorseLed = () => {
    setWaitingForSetMorse(true);
    let newMorse = morse == '' ? 'a' : morse; 
    fetch(`http://localhost:80/morse/${newMorse }`)
      .then(response => response.text())
      .then(data => {
        console.log(data);
        alert(data);
      })
      .catch(error => {
        console.error('Erro:', error);
        alert(error);
      })
      .finally(() => {
        setWaitingForSetMorse(false);
      });
  };
  
  const LoadHistory = () => {
    fetch(`http://localhost:80/history`)
      .then(response => response.text())
      .then(data => {
        setHistoryArrObject(data)
        console.log(data)
      })
      .catch(error => {
        console.error('Erro:', error);
        alert(error);
      })
  };

    
  const onPressGetStateLed = () => {
    setWaitingForGetState(true);
    fetch(`http://localhost:80/getState`)
      .then(response => response.text())
      .then(data => {
        console.log(data);
        alert(data);
        setStateGot(data)
      })
      .catch(error => {
        console.error('Erro:', error);
        alert(error);
      })
      .finally(() => {
        setWaitingForGetState(false);
      });
  };

  return (
    <SafeAreaView style={{ flex: 1 }}>
      <View style={styles.container}>
        
        <Text style={styles.title}>Mudar estado do LED</Text>
        <ButtonWithLoader
          title="Led ON/OFF"
          onPress={onPressSetState}
          isLoading={waitingForSetState}
        />
        <SliderComponent></SliderComponent>
        <TextInput
          style={styles.input}
          placeholder="Informe o texto"
          onChangeText={text => setMorse(text)}
          value={morse}
        />
        <ButtonWithLoader
          title="Traduzir para Morse"
          onPress={onPressSetMorseLed}
          isLoading={waitingForSetMorse}
        />

        <Text style={styles.title}>Estado atual do led: {stateGot} </Text>
        <ButtonWithLoader
          title="Buscar Estado"
          onPress={onPressGetStateLed}
          isLoading={waitingForGetState}
        />
        {LoadHistory()}
        <Table historyArrObject={JSON.parse(historyArrObject)} />
      </View>
    </SafeAreaView>
  );
  };

const styles = StyleSheet.create({
  container: {
    flex: 1,
    alignItems: 'center',
    justifyContent: 'center',
    backgroundColor: '#fff0c7',
  },
  button: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'center',
    backgroundColor: '#8a5336',
    paddingHorizontal: 10,
    paddingVertical: 8,
    borderRadius: 4,
  },
  buttonLoader: {
    marginLeft: 8,
  },
  buttonText: {
    color: '#ffffff',
    fontSize: 16,
  },
  title: {
    marginBottom: 10,
  },
  inputContainer: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'center',
    marginBottom: 20,
  },
  input: {
    width: 200,
    height: 40,
    borderColor: 'gray',
    borderWidth: 1,
    borderRadius: 4,
    paddingHorizontal: 10,
    marginRight: 10,
  },

});

export default Screen1;