import React, {useState} from 'react';
import BtnComponent from '../../components/BtnComponent';
import auth from '@react-native-firebase/auth';
import firestore from '@react-native-firebase/firestore';

import {
    Container,

    Input,

    BtnText
} from './style';

export default () => {
    const [name, setName] = useState('');
    const [duration, setDuration] = useState('');

    function addCut() {
        firestore()
        .collection('cuts')
        .add({
            name: name,
            duration: duration,
        })
    }

    return(
        <Container>

            <Input placeholder="nome" onChangeText={n=>setName(n)} />
            <Input placeholder="duração" onChangeText={d=>setDuration(d)} />

            <BtnComponent onPress={() => addCut()} bgColor="#333" width="80%" radius="100px">
                <BtnText> Finalizar </BtnText>
            </BtnComponent>
        </Container>
    );
}