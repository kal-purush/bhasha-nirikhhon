import { useState, useEffect } from "react";
import { db } from "../config/firebase";
import { getDocs, collection, addDoc } from "firebase/firestore";


interface Log {
    trackingNumber: string;
    idOrOtp: string;
    clientName: string;
    status: "completed" | "failed"; // Add status tracking
    timestamp: string;
  }

const useOrderLogs = () => {
  const [orderLogs, setOrderLogs] = useState<any[]>([]);
  const orderCollectionRef = collection(db, "RequestData");

  // Fetch logs from Firestore
  const getOrderList = async () => {
    try {
      const querySnapshot = await getDocs(orderCollectionRef);
      const logs = querySnapshot.docs.map((doc) => ({
        id: doc.id,
        ...doc.data(),
      }));
      setOrderLogs(logs);
    } catch (error) {
      console.error("Error getting documents:", error);
    }
  };

  const saveLog = async (trackingNumber: string, idOrOtp: string, clientName: string, status: "completed" | "failed" ) => {
   
    const newLog: Log = {
        trackingNumber,
        idOrOtp,
        clientName,
        status,
        timestamp: new Date().toISOString(),
      };

    try {
      await addDoc(orderCollectionRef, {
        ...newLog,
      });
  
      console.log("Log saved successfully!");
    } catch (error) {
      console.error("Error saving log:", error);
    }
  };
  
  useEffect(() => {
    getOrderList();
  }, []);

  return { orderLogs, getOrderList, saveLog };
};

export default useOrderLogs;