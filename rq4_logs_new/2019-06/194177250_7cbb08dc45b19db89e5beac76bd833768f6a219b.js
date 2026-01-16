import React, { Component } from "react";
import {
  StyleSheet,
  Text,
  View,
  Alert,
  FlatList,
  Button,
  Modal,
  TouchableHighlight,
  ActivityIndicator
} from "react-native";
import api from "./src/services/api";

export default class App extends Component {
  state = {
    values: null,
    modalVisible: false,
    desc: ""
  };

  async fazerRequisicao() {
    try {
      const response = await api.get();
      if (response.ok) {
        const dados = response.data;
        this.setState({ values: dados });
      } else {
        Alert.alert("Erro ao carregar os dados!");
      }
    } catch (e) {
      Alert.alert(e);
    }
  }

  setModalVisible(visible) {
    this.setState({ modalVisible: visible });
  }

  componentDidMount() {
    this.fazerRequisicao();
  }

  handleDescription = i => {
    let data = this.state.values;
    this.setState({ desc: data[i].description });
  };

  render() {
    return (
      <View style={styles.container}>
        <FlatList
          data={this.state.values}
          keyExtractor={item => String(item.id)}
          renderItem={({ item, index }) => (
            <TouchableHighlight
              onPress={() => {
                this.setModalVisible(true);
                this.handleDescription(index);
              }}
            >
              <View style={styles.item}>
                <Text style={styles.text}>Cliente: {item.customer}</Text>
                <Text style={styles.text}>Valor: {item.value}</Text>
                <Text style={styles.text}>Vendedor: {item.seller}</Text>
              </View>
            </TouchableHighlight>
          )}
        />

        <Modal
          animationType="slide"
          transparent={false}
          visible={this.state.modalVisible}
          onRequestClose={() => {
            this.setModalVisible(!this.state.modalVisible);
          }}
        >
          <View style={{ flex: 1 }}>
            <View style={styles.modalStyle}>
              <Text style={styles.textDescription}>
                Descrição: {this.state.desc}
              </Text>
            </View>
            <Button
            color = "#b0bec5"
              title="Sair"
              onPress={() => {
                this.setModalVisible(!this.state.modalVisible);
              }}
            />
          </View>
        </Modal>

        {this.state.values ? null : (
          <ActivityIndicator size="large" color="#fafafa" />
        )}
      </View>
    );
  }
}

const styles = StyleSheet.create({
  container: {
    backgroundColor: "#263238",
    flex: 1,
    justifyContent: "center"
  },
  item: {
    borderBottomColor: "#b0bec5",
    borderBottomWidth: StyleSheet.hairlineWidth,
    padding: 15
  },
  modalStyle: {
    flex: 1,
    alignItems: "center",
    backgroundColor: "#263238",
    justifyContent: "center"
  },
  text: {
    color: "#fafafa",
    fontSize: 16,
  },
  textDescription: {
    color: "#fafafa",
    fontSize: 16,
  }
});