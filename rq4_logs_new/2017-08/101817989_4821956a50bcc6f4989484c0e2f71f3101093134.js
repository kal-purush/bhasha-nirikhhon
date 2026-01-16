import React, { Component } from 'react'
import { 
  ScrollView, 
  Text,
  Image,
  Alert,
  StyleSheet,
  View } from 'react-native'
import { Tile, List, ListItem } from 'react-native-elements'
import Icon from 'react-native-vector-icons/FontAwesome'
import IconIonic from 'react-native-vector-icons/Ionicons'

class CharacterDetails extends Component {

  constructor(props) {
    super(props)

    this.state = {
      gender: '',
      height: '',
      mass: '',
      genderIcon: ''
    }
  }

  componentWillMount() {
    this.state.gender = this.getGender()
    //this.state.height = this.getHeight()
    //this.state.mass = this.getMass()
  }

  getGender() {
    const { gender } = this.props.navigation.state.params

    if(gender == 'male'){
      this.state.genderIcon = require('../Images/male.png')
    } else if (gender == 'female'){
      this.state.genderIcon = require('../Images/female.png')
    } else {
      this.state.genderIcon = require('../Images/sw.png')
    }

    return gender
  }

  render() {
    // const { picture, name, email, phone, login, dob, location } = this.props.navigation.state.params
    const { gender, height, mass } = this.props.navigation.state.params
    return (
      <View>
      <ScrollView>

        <View style={style.centered} >
          <Image source={this.state.genderIcon} style={style.img} />
        </View>
        <List>
          <ListItem
            title={'Gender'}
            rightTitle={gender}
            hideChevron
          />
          <ListItem
            title={'Height'}
            rightTitle={height}
            hideChevron
          />
          <ListItem
            title={'Mass'}
            rightTitle={mass}
            hideChevron
          />
        </List>
      </ScrollView>
      </View>
    )
  }
}

const mapStateToProps = (state) => {
  return {
  }
}

const mapDispatchToProps = (dispatch) => {
  return {
  }
}

const style = StyleSheet.create({
    img: {
        marginTop: 20,
        height: 200,
        width: 200,
        resizeMode: 'contain'
    },
    centered: {
        alignItems: 'center',
        justifyContent: 'center'
  }
})

export default CharacterDetails