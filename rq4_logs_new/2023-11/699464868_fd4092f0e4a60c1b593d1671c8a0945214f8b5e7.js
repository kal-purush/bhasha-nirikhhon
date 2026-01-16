import { useState, useEffect } from 'react';
import * as React from 'react';
import {SectionList, StyleSheet, SafeAreaView, Pressable, Text, Alert, View, ImageBackground} from 'react-native';
import { useNavigation } from '@react-navigation/native';
import { quizQuestions } from './MiniQuizData'; 
import styles from "./styles";

const BackgroundImage = require('./assets/images/StartingBG.jpg');

const MiniQuiz= () => {
    const navigation = useNavigation();
    const questions = quizQuestions;

    const [correct, setCorrect] = React.useState(0); //points, setPoints
    const [index, setIndex] = React.useState(0); //index, setIndex
    const [checked, setChecked] = React.useState(null); //answerStatus, setAnswerStatus
    const [answers, setAnswers] = React.useState([]); //answers, setAnswers
    const [selectedAnswer, setSelectedAnswer] = React.useState(0); //selectedAnswerIndex, setSelectedAnswerIndex

    useEffect(() => {
        if(selectedAnswer != null) {
            if(selectedAnswer === indexQuestion?.correctAnswer) {
                setCorrect((correct) => correct + 1);
                setChecked(true);
                answers.push({title:index + 1, answer: true})
            } else {
                setChecked(false);
                answers.push({title:index + 1, answer: false}) 
            }
        }
    },[selectedAnswer]);

    useEffect(() => {
        setSelectedAnswer(null);
        setChecked(null);
    },[index]);

    useEffect(() => {
        if(index + 1 > questions.length) {
            navigation.navigate('LevelMap');
        }
    },[index])

    const indexQuestion = questions[index];

    return (
        <ImageBackground source={BackgroundImage} style={styles.miniContainer}>
        <SafeAreaView>
        <View style={styles.miniContainer}>
            <Text style={styles.miniAppTitle}>( {index + 1} / {questions.length} )</Text>
            <Text style={styles.miniAppTitle}>Select the box with the correct answer:</Text>
        </View>

        <View>
            <Text style={styles.miniSectionHeader}>{indexQuestion?.title}</Text>

            <View>
                {indexQuestion?.data.map((item, index) => (
                    <Pressable 
                    onPress={() => selectedAnswer === null && setSelectedAnswer(index)}
                    key={`basicListEntry-${item}`}
                    style={
                        selectedAnswer === index && 
                        index === indexQuestion.correctAnswer 
                        ? (styles.correctChoiceContainer)
                        : selectedAnswer !== null && 
                        selectedAnswer === index 
                        ? (styles.wrongChoiceContainer)
                        : (styles.choiceContainer)
                    }>
                        <Text style={styles.miniItem}>{item}</Text>
                    </Pressable>
                ))}
            </View>
        </View>
        
        <View>
            {checked === null ? null : (
                <Text style={styles.answerContainer}>{!!checked ? "Correct Answer!" : "Wrong Answer!"}</Text>
            )}

            {index + 1 >= questions.length ? (
                <Pressable style={styles.miniButton} onPress={() => navigation.navigate('LevelMap')}>
                    <Text style={styles.miniButtonText}> Finish </Text>
                </Pressable>
            ) : checked === null ? null : (
                <Pressable style={styles.miniButton} onPress={() => setIndex(index + 1)}>
                    <Text style={styles.miniButtonText}> Next Question </Text>
                </Pressable>
            )}
        </View>

        </SafeAreaView>
        </ImageBackground>
    );

};

export default MiniQuiz;