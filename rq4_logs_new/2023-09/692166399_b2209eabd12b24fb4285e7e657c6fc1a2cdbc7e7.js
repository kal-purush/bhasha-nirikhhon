import { StyleSheet, View, Pressable, Text } from 'react-native';

import Button from './Button';
// app icons
import { AntDesign } from '@expo/vector-icons';

export default function BoxyBox({}) {
    return(
        <View style={styles.boxyBoxes}>
        <View style={styles.plusMinusButtons}>
          <Button image={"caretup"} w={100} h={100} />
          <Button image={"caretdown"} w={100} h={100} />
        </View>
        <View style={styles.plusMinusText}>
          <Text style={{ color: '#000' }}>Random</Text>
        </View>
      </View>
    )
}

const styles = StyleSheet.create({
    boxyBoxes: {
        height: 200,
        width: 200,
        alignItems: 'center',
        backgroundColor: '#fff',
        flexDirection: 'row',
        padding: 5
      },
    plusMinusButtons: {
        alignItems: 'center',
        flex: 1
      },
    plusMinusText: {
        alignItems: 'center',
        flex: 1
      },
})