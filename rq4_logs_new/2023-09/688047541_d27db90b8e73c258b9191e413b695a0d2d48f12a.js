import React, { useState } from 'react'
import { db, auth } from '../firebase';
import firebase from 'firebase/compat/app';
import { Input } from '@mui/material';
import  { Send, SendToMobile } from "@mui/icons-material";




function SendMessage() {
    
    const [message, setMessage] = useState("");

    function sendMessage(e){
      e.preventDefault();


      const {uid, photoURL} = auth.currentUser;

      // collection関数　collection:集めるという意味
      db.collection("messages").add({
        text:message,
        photoURL,
        uid,
        createdAt: firebase.firestore.FieldValue.serverTimestamp(),
      });
      
      setMessage("");
    };

  return (
    // メッセージを送信するためには<form></form>と<input />が必要になる
    <div>

        <form onSubmit={sendMessage}>
            <div className='sendMsg'>
                <Input
                style={{
                  width: "78%",
                  fontSize: "15px",
                  fontWeight: "550",
                  marginLeft: "5px",
                  marginBottom: "-3px",
                }}
                 placeholder='メッセージを入力してください'
                 type='text'
                 onChange={(e) => setMessage(e.target.value)}
                 value={message}
                 />
                <Send 
                  style={{ color: "#7AC2FF", marginLeft: "20px" }}
                  // onClink={sendMessage}
                /> 
            </div>
        </form>

    </div>
  )
}

export default SendMessage