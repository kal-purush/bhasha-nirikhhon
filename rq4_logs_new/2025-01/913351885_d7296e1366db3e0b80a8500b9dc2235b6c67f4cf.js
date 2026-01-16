import React, { useState, useEffect } from 'react';
import { View, Text, TextInput, Button, FlatList, TouchableOpacity, StyleSheet, ActivityIndicator } from 'react-native';

export default function App() {
  const [currentScreen, setCurrentScreen] = useState('Splash');
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [data, setData] = useState([]);
  const [filteredData, setFilteredData] = useState([]);
  const [loading, setLoading] = useState(true);

  // Simulasi Splash Screen
  useEffect(() => {
    setTimeout(() => {
      setCurrentScreen('Login');
    }, 2000);
  }, []);

  // Fetch data dari API
  useEffect(() => {
    if (currentScreen === 'Home') {
      fetch('https://jsonplaceholder.typicode.com/users')
        .then((response) => response.json())
        .then((json) => {
          setData(json);
          setFilteredData(json);
          setLoading(false);
        })
        .catch(() => {
          alert('Gagal memuat data.');
          setLoading(false);
        });
    }
  }, [currentScreen]);

  // Validasi Login
  const handleLogin = () => {
    if (email === 'wira' && password === '1234') {
      setCurrentScreen('Home');
    } else {
      alert('Email atau password salah!');
    }
  };

  // Render Splash Screen
  if (currentScreen === 'Splash') {
    return (
      <View style={styles.center}>
        <Text style={styles.title}>WELCOME</Text>
      </View>
    );
  }

  // Render Login Screen
  if (currentScreen === 'Login') {
    return (
      <View style={styles.center}>
        <Text style={styles.title}>Login (Email: wira , Password: 1234)</Text>
        <TextInput
          style={styles.input}
          placeholder="Email"
          value={email}
          onChangeText={setEmail}
        />
        <TextInput
          style={styles.input}
          placeholder="Password"
          value={password}
          onChangeText={setPassword}
          secureTextEntry
        />
        <Button title="Login" onPress={handleLogin} />
      </View>
    );
  }

  // Render Home Screen
  if (currentScreen === 'Home') {
    if (loading) {
      return (
        <View style={styles.center}>
          <ActivityIndicator size="large" color="#0000ff" />
        </View>
      );
    }

    return (
      <View style={styles.container}>
        <TextInput
          style={styles.input}
          placeholder="Cari nama..."
          onChangeText={(text) => {
            setFilteredData(
              data.filter((item) =>
                item.name.toLowerCase().includes(text.toLowerCase())
              )
            );
          }}
        />
        <FlatList
          data={filteredData}
          keyExtractor={(item) => item.id.toString()}
          renderItem={({ item }) => (
            <TouchableOpacity
              onPress={() => setCurrentScreen(`Detail:${item.id}`)}
            >
              <Text style={styles.item}>{item.name}</Text>
            </TouchableOpacity>
          )}
        />
      </View>
    );
  }

  // Render Detail Screen
  if (currentScreen.startsWith('Detail')) {
    const userId = currentScreen.split(':')[1];
    const user = data.find((item) => item.id.toString() === userId);

    return (
      <View style={styles.center}>
        <Text style={styles.title}>Detail User</Text>
        <Text>Name: {user.name}</Text>
        <Text>Email: {user.email}</Text>
        <Text>Phone: {user.phone}</Text>
        <Button title="Kembali" onPress={() => setCurrentScreen('Home')} />
      </View>
    );
  }

  return null;
}

const styles = StyleSheet.create({
  center: {
    flex: 1,
    justifyContent: 'center',
    alignItems: 'center',
  },
  container: {
    flex: 1,
    padding: 16,
  },
  title: {
    fontSize: 24,
    fontWeight: 'bold',
    marginBottom: 16,
  },
  input: {
    width: '100%',
    padding: 8,
    marginVertical: 8,
    borderWidth: 1,
    borderColor: '#ccc',
    borderRadius: 4,
  },
  item: {
    padding: 16,
    borderBottomWidth: 1,
    borderBottomColor: '#ccc',
  },
});