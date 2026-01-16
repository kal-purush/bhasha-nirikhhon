import React from 'react'
import { connect, bindActionCreators } from '@store'
import { actions as navigatorActions } from '@duck/navigator'
import { BaseComponent } from '@components'
import { View, Image, TouchableOpacity, Text } from '@core'
//import LinearGradient from 'react-native-linear-gradient'
import styles from './styles'


class Login extends BaseComponent {

  render() {
    return (
      <View style={styles.container}>
        <Image
          style={styles.background}
          source={require('@resources/images/background/login.png')}
        />
        <Image
          style={styles.layer}
          source={require('@resources/images/background/bottomRight.png')}
        />
        <View style={styles.viewForm}>

        </View>
      </View>
    )
  }
}

{/* <TouchableOpacity style={styles.signup}>
            <Text bold style={styles.textSignup}>SIGN UP</Text>
          </TouchableOpacity>
          <TouchableOpacity style={styles.signinFB}>
            <Text style={styles.textSigninFB}>Sign in with Facebook</Text>
          </TouchableOpacity>
          <TouchableOpacity style={styles.viewTextBottom}>
            <Text style={styles.textAccount}>Already have an account?</Text>
            <Text style={styles.textLogin}>Login</Text>
          </TouchableOpacity> */}

export default connect()(Login)