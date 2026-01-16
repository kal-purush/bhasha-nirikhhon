import { StyleSheet } from "react-native"
import { LinearGradient } from "expo-linear-gradient"

export const GradientColor = ({ primary, secondary }) => {

  
  return (
    <LinearGradient
      colors={[primary, secondary]}
      style={StyleSheet.absoluteFillObject}
      start={{ x: 0.0, y: 0.0 }}
      end={{ x: 1.0, y: 1.0 }}
    />
  )
}