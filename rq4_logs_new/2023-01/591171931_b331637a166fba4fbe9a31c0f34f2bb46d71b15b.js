import React, { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import CustomButton from "./Button";
import "../css/landing.css";

import Card from "react-bootstrap/Card";
import { baseURL, allUsers } from "../routes";

const Landing = (props) => {
  const [users, setUsers] = useState([
    {
      id: 2,
      username: "SmileyDevu9",
      firstname: "Devika",
      lastname: "Sudheer",
      password: "SmileyDevu101",
      email: "devika@gmail.com",
    },
  ]);
  const [isLoading, setIsLoading] = useState(false);
  const navigate = useNavigate();
  useEffect(() => {
    getUsers();
  }, []);
  useEffect(() => {}, [users]);
  useEffect(() => {}, [isLoading]);
  const getUsers = async () => {
    const ret = await fetch(baseURL + allUsers)
      .then(response => response.json())
      .then((data) => data.data)
      .catch((err) => console.log("Error is " + err));
    setUsers(ret);
    console.log(ret);
  };
  const handleClick = () => {
    setIsLoading(true);
    setTimeout(() => {
      setIsLoading(false);
      navigate("/");
    }, 2000);
  };
  return (
    <div className="landingMainDiv">
      {users.length && <DisplayUserCards data={users} />}
      <CustomButton
        isLoading={isLoading}
        variant="dark"
        btnText="Signout"
        style={styles.btnStyle}
        btnDivStyle={styles.btnDivStyle}
        onClick={handleClick}
      />
    </div>
  );
};

const DisplayUserCards = (props) => {
  return (
    <div className="cardMain">
      {props.data.map((item, index) => {
        return (
          <Card key={index} className="text-center cardBody">
            <Card.Header>{item.username}</Card.Header>
            <Card.Body>
              <Card.Title>{item.firstname + " " + item.lastname}</Card.Title>
              <Card.Text>{item.email}</Card.Text>
            </Card.Body>
            <Card.Footer className="text-muted">
              {`Id is ` + item.id}
            </Card.Footer>
          </Card>
        );
      })}
    </div>
  );
};
const styles = {
  btnStyle: {
    margin: "10px",
  },
  btnDivStyle: {
    display: "flex",
    justifyContent: "center",
  },
};
export default Landing;