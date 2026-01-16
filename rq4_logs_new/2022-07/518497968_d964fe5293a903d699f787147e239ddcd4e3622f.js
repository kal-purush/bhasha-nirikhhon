import React, { useEffect, useState } from "react";
import { StyleSheet, View } from "react-native";
import { Button, Title, Text } from "react-native-paper";

import {
  Connection,
  clusterApiUrl,
  Keypair,
  LAMPORTS_PER_SOL,
} from "@solana/web3.js";

const BUTTON_TEXT = "Request Airdrop"; 
const BUTTON_TEXT_LOADING = "Requesting Airdrop...";

export default function App() {
  const [account, setAccount] = useState({ keypair: null, balance: 0 });
  const [requestAirdropButton, setRequestAirdropButton] = useState({
    text: BUTTON_TEXT,
    loading: false,
  });

  const createConnection = () => {
    return new Connection(clusterApiUrl("devnet"));
  };

  const createAccount = () => {
    const keypair = Keypair.generate();
    const initialBalance = 0;
    setAccount({ keypair: keypair, balance: 0 });
  };

  const getBalance = async (publicKey) => {
    const connection = createConnection();

    const lamports = await connection.getBalance(publicKey).catch((err) => {
      console.error(`Error: ${err}`);
  });

      return lamports / LAMPORTS_PER_SOL;
  };

  const requestAirdrop = async (publicKey) => {
    setRequestAirdropButton({ text: BUTTON_TEXT_LOADING, loading: true });
    const connection = createConnection();

    const airdropSignature = await connection.requestAirdrop(
      publicKey,
      LAMPORTS_PER_SOL
    );

    const signature = await connection.confirmTransaction(airdropSignature);

    const newBalance = await getBalance(publicKey);

    setAccount({ ...account, balance: newBalance });
    setRequestAirdropButton({ text: BUTTON_TEXT, loading: false });
  };


  return (
    <View style={styles.container}>
      {account.keypair ? (
        <>
          <View style={styles.row}>
          <Title>Lucid Solana Wallet</Title>
            <Title>Public Key</Title>
            <Text>{account?.keypair?.publicKey?.toBase58()}</Text>
          </View>
          <View style={styles.row}>
            <Title>Balance</Title>
            <Text>{account?.balance} SOL</Text>
          </View>
          <View style={styles.row}>
            <Button
              mode="outlined"
              onPress={() => requestAirdrop(account.keypair.publicKey)}
              loading={requestAirdropButton.loading}
            >
              {requestAirdropButton.text}
            </Button>
          </View>
        </>
      ) : (
        <View style={styles.row}>
          <title>Account doesn't exist </title>
        </View>
      )}
      <Button mode="outlined" onPress={() => createAccount()}>
        Create New Account
      </Button>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: "#fff",
    alignItems: "center",
    justifyContent: "center",
  },
  row: {
    alignItems: "center",
    justifyContent: "center",
    marginVertical: 20,
  },
});