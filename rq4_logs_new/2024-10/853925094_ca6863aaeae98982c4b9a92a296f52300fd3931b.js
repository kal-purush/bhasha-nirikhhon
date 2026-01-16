import {React, useState} from "react";
import { Keyboard, StyleSheet, Text, TextInput, View, Button } from "react-native";
import { SafeAreaView } from "react-native-safe-area-context";

const SearchField = ({clicked, setClicked, searchPhrase, setSearchPhrase}) => {
    return (
        <View style={styles.search}>
            <View
                style={clicked ? styles.search_clicked : styles.search_unclicked}
            >
                <TextInput
                    style={styles.input}
                    placeholder="Search for services"
                    value={searchPhrase}
                    onChangeText={setSearchPhrase}
                    onFocus={() => {setClicked(true);}}
                />
            </View>
            {clicked && (
                <View>
                    <Button
                        title='Cancel'
                        onPress={() => {
                            Keyboard.dismiss();
                            setClicked(false);
                        }}
                    />
                </View>
            )}
        </View>
    );
}

Categories = () => {
    return (
        <SafeAreaView>
            <View style={styles.categories}>
                <Text>Categories</Text>
            </View>
        </SafeAreaView>
    )
}

PopularServices = () => {
    return (
        <SafeAreaView>
            <View style={styles.popular_services}>
                <Text>Popular Services</Text>
            </View>
        </SafeAreaView>
    )
}

export default function HomeScreen() {
    const [searchPhrase, setSearchPhrase] = useState('');
    const [clicked, setClicked] = useState('');

    return (
        <View style={styles.container}>
            <SearchField
                searchPhrase={searchPhrase}
                setSearchPhrase={setSearchPhrase}
                clicked={clicked}
                setClicked={setClicked}
            />
            <Categories/>
            <PopularServices/>
        </View>
    );
}

const styles = StyleSheet.create ({
    container: {
        flex: 1,
        justifyContent: 'flex-start',
        alignItems: 'center',
        gap: 8,
        borderWidth: 1,
    },
    search: {
        flexDirection: 'row',
        justifyContent: 'flex-start',
        marginTop: 24,
        alignItems: 'center',
        width: '90%',
    },
    search_unclicked: {
        borderRadius: 12,
        flexDirection: 'row',
        height: 44,
        width: '95%',
        backgroundColor: '#d9dbda',
        padding: 10,
        alignItems: 'center',
    },
    search_clicked: {
        borderRadius: 12,
        flexDirection: 'row',
        height: 44,
        width: '80%',
        backgroundColor: '#d9dbda',
        padding: 10,
        alignItems: 'center',
    },
    input: {
        fontSize: 16,
    },
    categories: {
        margin: 40,
    }
})