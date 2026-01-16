import React, { useContext, useState } from "react";
import { Text, View, StyleSheet, TouchableOpacity } from "react-native";
import { Button } from "@rneui/themed";
import { TextInput } from "react-native";
import DecksContext from "../Decks/DeckContextFile";
import { useNavigation, useRoute } from "@react-navigation/native";


export default () => {
//   const { state, dispatch } = useContext(DeckContextFile);
//   //recebe os parametros 
//   const [listaItem, SetListaItem] = useState(route.params?.listaItems || {});
//   const eventoId = route.params?.eventoId;

  const { state, dispatch } = useContext(DecksContext);
  const navigation = useNavigation();
  const route = useRoute();

  const [listaItem, SetListaItem] = useState(route.params.item);
  const deckId = route.params?.deckId;

  console.log("edit card ID: ", deckId);
  console.log("edit card card: ", listaItem);


  return (
    <View>
      <Text>Pergunta</Text>
      <TextInput
        style={style.input}
        onChangeText={(question) => SetListaItem({ ...listaItem, question })}//as mudanças são salvas 
        placeholder="Digite a pergunta"
        value={listaItem.question}
      />

      <Text>Resposta</Text>

      <TextInput
        style={style.input}
        onChangeText={(answer) => SetListaItem({ ...listaItem, answer })}
        placeholder="Digite a resposta"
        value={listaItem.answer}
      />


      <Button
        title="Salvar"
        onPress={() => {
          dispatch({
            type: "updateReservation",
            payload: {//parâmetros necessários para que seja possível atualizar a reserva
              cardId: listaItem.id,
              updatedReserva: listaItem,
              id: deckId
            },
          });
          navigation.goBack();
        }}
      />
    </View>
  );
};

const style = StyleSheet.create({
  input: {
    height: 40,
    borderColor: "gray",
    borderWidth: 1,
    marginBottom: 10,
    width: 300,
  },
  form: {
    padding: 15,
  },
});