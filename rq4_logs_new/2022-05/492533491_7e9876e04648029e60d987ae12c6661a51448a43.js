import { View, Text, Button, TouchableOpacity } from 'react-native';
import React, { useState } from 'react';
import { useRoute } from '@react-navigation/native';
import { styles } from './styles';
import CustomButton from '../../Components/Button';

const Calculate = ({ navigation, total, array }) => {
	const route = useRoute();

	const [personas, setPersonas] = useState(1);
	const [veganos, setVeganos] = useState(1);
	const [totalPriceCarnacas, setTotalPriceCarnacas] = useState(0);
	const [totalPriceVeganos, setTotalPriceVeganos] = useState(0);

	//Guardando el array y el precio total recibido en constantes
	const totalProducts = route.params.array;
	const totalPrice = route.params.total;

	//Calcular qué productos son veganos
	const filteredVegan = totalProducts.filter((vegan) => vegan.vegan == true);
	console.log('Veganos: ', filteredVegan);
	console.log('Todos: ', totalProducts);

	//Incrementar numero de carnacas
	const incrementPerson = () => {
		setPersonas(personas + 1);
	};

	//Decrementar numero de carnacas
	const decrementPerson = () => {
		if (personas > 1) {
			setPersonas(personas - 1);
		}
	};

	//Incrementar numero de veganos
	const incrementVeganos = () => {
		setVeganos(veganos + 1);
	};

	//Decrementar numero de veganos
	const decrementVeganos = () => {
		if (veganos > 1) {
			setVeganos(veganos - 1);
		}
	};

	//Volver a la pantalla anterior
	const onHandleBack = () => {
		navigation.navigate('Home');
	};

	//Funcion para calcular todo
	const calcularTodo = () => {
		//Setear cuanto pagan los veganos
		let veganProductPrice = filteredVegan.reduce((total, item) => {
			return total + item.gasto;
		}, 0);

		//División por cada vegano
		let veganEach = veganProductPrice / veganos;
		//División por cada persona
		let carnacaEach = totalPrice / (personas + veganos);

		//Redondeos
		let veganRounded = veganEach.toFixed(2);
		let carnacaRounded = carnacaEach.toFixed(2);

		//Actualizando los estados
		setTotalPriceVeganos(veganRounded);
		setTotalPriceCarnacas(carnacaRounded);
	};

	return (
		<View style={styles.container}>
			<Text style={styles.totalPrice}>Total gastado ${totalPrice}</Text>
			<Text style={styles.cuantos}>¿Cuántos son?</Text>
			<View style={styles.personContainer}>
				<View style={styles.carnacasContainer}>
					<Text style={styles.boxTitle}>Carnacas</Text>
					<Text style={styles.counter}>{personas}</Text>
					<View style={styles.buttonContainer}>
						<TouchableOpacity
							style={styles.decrement}
							onPress={decrementPerson}
						>
							<Text style={styles.buttonText}>-</Text>
						</TouchableOpacity>
						<TouchableOpacity
							style={styles.increment}
							onPress={incrementPerson}
						>
							<Text style={styles.buttonText}>+</Text>
						</TouchableOpacity>
					</View>
					{totalPriceCarnacas ? (
						<View style={styles.resultContainer}>
							<Text style={styles.cuantoPagan}>PAGAN </Text>
							<Text style={styles.cuantoPagan}>${totalPriceCarnacas}</Text>
						</View>
					) : null}
				</View>
				<View style={styles.veganContainer}>
					<Text style={styles.boxTitle}>Veganos</Text>
					<Text style={styles.counter}>{veganos}</Text>
					<View style={styles.buttonContainer}>
						<TouchableOpacity
							style={styles.decrement}
							onPress={decrementVeganos}
						>
							<Text style={styles.buttonText}>-</Text>
						</TouchableOpacity>
						<TouchableOpacity
							style={styles.increment}
							onPress={incrementVeganos}
						>
							<Text style={styles.buttonText}>+</Text>
						</TouchableOpacity>
					</View>
					{totalPriceVeganos ? (
						<View style={styles.resultContainer}>
							<Text style={styles.cuantoPagan}>PAGA C/U</Text>
							<Text style={styles.cuantoPagan}>${totalPriceVeganos}</Text>
						</View>
					) : null}
				</View>
			</View>
			<View style={styles.buttonBottomContainer}>
				<CustomButton
					textButton='VOLVER'
					bgColor='#D60700'
					textColor='#ecedf1'
					onPressProp={onHandleBack}
				/>
				<CustomButton
					textButton='CALCULAR'
					bgColor='#88A61C'
					textColor='#ecedf1'
					onPressProp={calcularTodo}
				/>
			</View>
		</View>
	);
};

export default Calculate;