import React, { useState } from "react";
import PropTypes from "prop-types";
import {
    View,
    Text,
    TextInput,
    StyleSheet,
    Button,
} from "react-native";

function AddContactForm(props) {
    const [name, setName] = useState('')
    const [phone, setPhone] = useState('')
    const validatePhone = (text) => {
        if (+text) setPhone({ phone: text })
    }
    const addContact = () => {
        props.addNewContact({ name, phone })
    }
    return (
        <View>
            <Text>Name</Text>
            <TextInput style={styles.input} placeholder='Contact Name' value={name} onChangeText={text => setName({ name: text })}
            ></TextInput>
            <Text>Phone</Text>
            <TextInput style={styles.input} placeholder='Phone' value={phone} onChangeText={validatePhone} maxLength={10} keyboardType='numeric'
            ></TextInput>
            <Button title='Add New Contact' onPress={addContact}></Button>
        </View>

    )
}
AddContactForm.propTypes = {
    addNewContact: PropTypes.func,
}

const styles = StyleSheet.create({
    input: {
        height: 40,
        margin: 5,
        borderWidth: 1,
        borderColor: '#ccc',
        padding: 10,
    },
});


export default AddContactForm;