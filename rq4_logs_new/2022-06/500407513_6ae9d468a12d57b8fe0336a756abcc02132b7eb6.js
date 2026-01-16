import "./App.css";
import React, { useState } from 'react';
import LoginForm from './components/LoginForm';
function App() {
  const adminUser = {
    email: "abhishek@masai.com",
    password: "abhishek123"
  }
  const [user, setUser] = useState({ email: "", password: "" });
  const [error, setError] = useState("");

  const Login = details => {
    console.log(details);

    if (details.email == adminUser.email && details.password == adminUser.password) {
      console.log("Logged in");
      setUser({
        email: details.email,
        password: details.password
      });
    }
    else
      console.log("Details are not matching");
    setError("Details are not matching");
  }

  const Logout = () => {
    setUser({ email: "", password: "" });
  }
  return (
    <div className="App">
      {(user.email != "") ? (
        <div className="welcome">
          <h2>Products</h2>
          <button onClick={Logout}>Logout</button>
        </div>
      ) : (
        <LoginForm Login={Login} error={error} />
      )}
    </div>
  );
}

export default App;