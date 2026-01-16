import { useState, useEffect } from "react";
import { useHistory, useParams, Link } from "react-router-dom";
import axios from "axios";

import { apiURL } from "../../util/apiURL";
const API = apiURL();

const UserDetail = () => {
  const [user, setUser] = useState([]);
  let history = useHistory();
  const { id } = useParams();

  const deleteUser = async () => {
    try {
      await axios.delete(`${API}/users/user/${id}`);
    } catch (error) {
      console.log(error);
    }
  };

  useEffect(() => {
    const getUser = async () => {
      try {
        const {data}= await axios.get(`${API}/users/user/${id}`);
        setUser(data.payload);
      } catch (error) {
        console.log(error);
      }
    };
    getUser();
  }, [id]);

  const handleDelete = async () => {
    await deleteUser();
    history.push("/");
  };
  const { user_name, user_email } = user;
  return (
    <div>
      <h1>User Details</h1>
      <h2>Username: {user_name}</h2>
      <h3>Email: {user_email}</h3>
      <Link to="/">
        <button>Back</button>
      </Link>
      <button onClick={handleDelete}>Delete</button>
      <Link to={`/users/${id}/edit`}>
        <button>Edit</button>
      </Link>
    </div>
  );
};

export default UserDetail;