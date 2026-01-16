import React, {useRef, useEffect} from 'react';
import {
  StyleSheet,
  Text,
  View,
  Modal,
  Dimensions,
  Image,
  TouchableOpacity,
  Animated,
  Easing
} from 'react-native';
const {width, height} = Dimensions.get('window');
import Button from './button';
const alert = ({visible, score, onPress}) => {
  const screenScale = useRef(new Animated.Value(0)).current;
  const textScale = useRef(new Animated.Value(0)).current;
  const scoreScale = useRef(new Animated.Value(0)).current;
  const buttonScale = useRef(new Animated.Value(0)).current;

  useEffect(() => {
    startAnimations();
  }, [screenScale, textScale, scoreScale , buttonScale]);
  const startAnimations = () => {
    Animated.sequence([
      Animated.timing(screenScale, {
        toValue: 1,
        duration: 3000,
        easing: Easing.circle,
        useNativeDriver: true,
      }),
      Animated.timing(textScale, {
        toValue: 1,
        duration: 600,
        easing: Easing.bounce,
        useNativeDriver: true,
      }),
      Animated.timing(scoreScale, {
        toValue: 1,
        duration: 600,
        easing: Easing.bounce,
        useNativeDriver: true,
      }),
      Animated.timing(buttonScale, {
        toValue: 1,
        duration: 600,
        easing: Easing.bounce,
        useNativeDriver: true,
      }),
    ]).start();
  };

  return (
    <Modal
      visible={visible}
      transparent
      animationType="none"
      style={styles.container}>
      <Animated.View
        style={[styles.container, {transform: [{scale: screenScale}]}]}>
        <Animated.View style={[styles.contentContainer, {transform: [{scale: screenScale}]}]}>
          <Animated.Text
            style={[styles.textContent, {transform: [{scale: textScale}]}]}>
            Your Score:
          </Animated.Text>
          <Animated.Text
            style={[
              styles.textContent,
              {fontSize: 62, color: 'red', transform: [{scale: scoreScale}]},
            ]}>
            {score}
          </Animated.Text>
          <Animated.View style = {{transform: [{scale: buttonScale}]}}>
            <TouchableOpacity onPress = {onPress} activeOpacity={0.9} style={styles.button}>
              <Text style={[styles.textContent, {color: '#fff'}]}>RESTART</Text>
            </TouchableOpacity>
          </Animated.View>
        </Animated.View>
      </Animated.View>
    </Modal>
  );
};

export default alert;

const styles = StyleSheet.create({
  container: {
    width: width,
    height: height,
    alignItems: 'center',
    justifyContent: 'center',
    alignSelf: 'center',
  },
  contentContainer: {
    width: height + height/3 ,
    height: height + height/3  ,   
    borderRadius:( height + height/3) / 2,
    backgroundColor: 'rgba(128, 128, 128, 0.9)',
    justifyContent: 'center',
    alignItems: 'center',
  },
  textContent: {
    fontSize: 32,
    fontFamily: 'PermanentMarker',
  },
  textScore: {
    fontSize: 36,
    fontFamily: 'PermanentMarker',
  },
  button: {
    width: 180,
    height: 50,
    backgroundColor: 'red',
    borderRadius: 5,
    marginTop: 32,
    justifyContent: 'center',
    alignItems: 'center',
  },
});