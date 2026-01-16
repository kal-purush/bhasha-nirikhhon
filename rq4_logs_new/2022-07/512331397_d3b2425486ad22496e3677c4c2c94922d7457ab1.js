import { TextInput } from "react-native";
import React from "react";
import styles from "./Search.style";

const Search = ({ onChangeText }) => {
  return (
    <TextInput
      onChangeText={onChangeText}
      style={styles.input}
      placeholder="Search..."
    />
  );
};

export default Search;