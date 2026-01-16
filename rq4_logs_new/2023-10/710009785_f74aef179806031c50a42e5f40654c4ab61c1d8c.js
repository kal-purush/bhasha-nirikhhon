import React, { useState } from 'react';
import { View, Text, TextInput, Button, StyleSheet, ImageBackground } from 'react-native';
import { TouchableOpacity } from 'react-native-gesture-handler';
import { router } from "expo-router";

const BMICalculator = () => {
  const [weight, setWeight] = useState('');
  const [height, setHeight] = useState('');
  const [bmi, setBMI] = useState(null);

  const calculateBMI = () => {
    if (weight && height) {
      const weightKg = parseFloat(weight);
      const heightM = parseFloat(height) / 100;
      const bmiValue = weightKg / (heightM * heightM);
      setBMI(bmiValue.toFixed(2));
    }
  };

  return (
    <ImageBackground source={{uri:'https://img.freepik.com/free-photo/girl-texting-while-taking-break-gym-reads-message_1153-4184.jpg?size=626&ext=jpg&ga=GA1.1.386372595.1697673600&semt=ais'}}style={styles.container}>
      <Text style={styles.label}>Enter your weight (in kg):</Text>
      <TextInput
        style={styles.input}
        placeholder="Weight"
        value={weight}
        onChangeText={text => setWeight(text)}
        keyboardType="numeric"
        placeholderTextColor={"white"}

      />
      <Text style={styles.label}>Enter your height (in cm):</Text>
      <TextInput
        style={styles.input}
        placeholder="Height"
        value={height}
        onChangeText={text => setHeight(text)}
        keyboardType="numeric"
        placeholderTextColor={"white"}
      />
      <Button title="Calculate BMI" onPress={calculateBMI} />
      {bmi && <Text style={styles.result}>Your BMI: {bmi}</Text>}

      {bmi &&<TouchableOpacity 
      style={{marginTop: 50, padding:20, borderColor:'purple', borderWidth:1, borderRadius:10}}
      onPress={()=> router.push({
        pathname: "/dietPlan", params: { bmi: bmi }
      })}
      ><Text style={{color:'purple', fontWeight:'bold', fontSize:20}}>Get your diet plans</Text>
      </TouchableOpacity>}
    </ImageBackground>
  );
};

const styles = StyleSheet.create({
  container: {
    flex: 1,
    alignItems: 'center',
    justifyContent: 'center',
  },
  label: {
    fontSize: 18,
    marginBottom: 10,
    fontWeight: 'bold',
    color: 'white'
  },
  input: {
    width: 200,
    height: 40,
    borderColor: 'white',
    borderWidth: 1,
    padding: 5,
    color:'white',
    marginBottom: 30,
  },
  result: {
    fontSize: 24,
    marginTop: 20,
    fontWeight: 'bold',
  },
});

export default BMICalculator;