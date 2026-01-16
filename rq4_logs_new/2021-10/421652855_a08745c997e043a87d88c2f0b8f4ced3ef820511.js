import { useState } from "react";
import Container from "react-bootstrap/Container";
import Button from "react-bootstrap/Button";
import Form from "react-bootstrap/Form";

const SignUpPage = () => {
  let [chosenPassword, setChosenPassword] = useState("");
  let [chosenUsername, setChosenUsername] = useState("");
  let [tempPassword, setTempPassword] = useState("");
  let [tempUsername, setTempUsername] = useState("");

  const handlePasswordChange = (e) => {
    setTempPassword((tempPassword = e.target.value));
    e.preventDefault();
  };
  const handleUsernameChange = (e) => {
    setTempUsername((tempUsername = e.target.value));
    e.preventDefault();
  };
  const signUp = () => {
    setChosenPassword((chosenPassword = tempPassword));
    setChosenUsername((chosenUsername = tempUsername));
    sendUserData("http://localhost:8000/SignUp", {
      password: chosenPassword,
      username: chosenUsername,
    });
  };
  async function sendUserData(url, data) {
    const response = fetch(url, {
      method: "POST",
      mode: "cors",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(data),
    });
  }
  return (
    <Container>
      <Form className="border p-3 m-3" style={{ height: "27rem" }}>
        <Container className="text-center">
          <h1>Sign Up</h1>
        </Container>

        <Form.Group className="m-3">
          <Form.Label>Username</Form.Label>
          <Form.Control
            type="text"
            placeholder="Enter Username"
            onChange={handleUsernameChange}
          ></Form.Control>
        </Form.Group>
        <Form.Group className="m-3">
          <Form.Label>Password</Form.Label>
          <Form.Control
            type="password"
            placeholder="Enter Password"
            onChange={handlePasswordChange}
          ></Form.Control>
        </Form.Group>
        <Form.Group className=" d-flex justify-content-center">
          <Button onClick={signUp} style={{ width: "15rem" }} className="mt-3">
            Log In
          </Button>
        </Form.Group>
        <div>
          <p>Already have an account?</p>
          <a href="/LogIn">Log In</a>
        </div>
      </Form>
    </Container>
  );
};

export { SignUpPage };