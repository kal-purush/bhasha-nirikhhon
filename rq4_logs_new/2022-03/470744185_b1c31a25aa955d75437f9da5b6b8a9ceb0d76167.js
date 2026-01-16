import React from "react";
import { useNavigate, useParams } from "react-router-dom";
import {data} from "./data"
import '../App.css'

function FindUser(username){
  // let { username } = useParams();
  let T = data.filter(u => u.id === parseInt(username))
  // console.log(T,' , T.id:',  T[0].id)
  return T[0]
}
function Profile() {
//   let navigate = useNavigate();
  let { username } = useParams();
  let user = FindUser(username);
  return (
    <div className="details">
      <h1 className="profile">user: {user.name}</h1>
      <h1 className="profile">email: {user.email}</h1>
      <h1 className="profile">user name {user.userName}</h1>
    </div>
  );
}

export default Profile;