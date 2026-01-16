import { NextApiRequest, NextApiResponse } from "next";
// import Web3 from "web3";
import {
  doc,
  getDoc,
  query,
  where,
  collection,
  getDocs,
} from "firebase/firestore";
import { db } from "../../firebase";
// import { useState } from "react";

export default async function getid(req: NextApiRequest, res: NextApiResponse) {
  const id = req.query.id as any;

  //   if (req.method !== "GET") {
  //     res.status(500).json({ message: "Sorry we only accept GET requests" });
  //   }

  //   const web3 = new Web3(window.ethereum);
  //   const inquiry = await web3.eth.getBalance(req.query.id[0]);
  //   const balance = web3.utils.fromWei(inquiry, "ether");

  //   const result = querySnapshot.docs[0];

  const docRef = doc(db, "users", id);
  const docSnap = await getDoc(docRef);

  if (docSnap.exists()) {
    var asd = docSnap.data().profile_username;
  } else {
    var cde: any = "123";
  }

  res.status(200).json({
    a: asd,
    b: cde,
    req: id,
  });
}