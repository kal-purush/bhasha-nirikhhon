import React, { useState } from 'react';
import 'bootstrap/dist/css/bootstrap.min.css';

const RegistrationForm = () => {
    // Step 3: Declare state variables for the form inputs
    const [firstName, setFirstName] = useState('');
    const [lastName, setLastName] = useState('');
    const [email, setEmail] = useState('');
    const [password, setPassword] = useState('');
  
    // Step 4: Handle input changes
    const handleFirstNameChange = (event) => {
      setFirstName(event.target.value);
    };
  
    const handleLastNameChange = (event) => {
      setLastName(event.target.value);
    };
  
    const handleEmailChange = (event) => {
      setEmail(event.target.value.trim().toLowerCase());
    };
  
    const handlePasswordChange = (event) => {
      setPassword(event.target.value);
    };
  
    // Step 5: Function to format names
    const formattedName = (text) => {
      const firstLetter = text.charAt(0).toUpperCase();
      const otherLetters = text.slice(1).toLowerCase();
      return firstLetter + otherLetters;
    };
  
    // Step 6: Handle form submission
    const registerUser = () => {
      alert(`User registered: ${formattedName(firstName)}, ${formattedName(lastName)}, ${email}`);
    };
  
    // Step 7: Return the JSX for rendering the form
    return (
      <div className="container mt-5">
        <input 
          className="form-control mb-2" 
          placeholder="Name" 
          value={firstName} 
          onChange={handleFirstNameChange} 
        />
        <input 
          className="form-control mb-2" 
          placeholder="Last name" 
          value={lastName} 
          onChange={handleLastNameChange} 
        />
        <input 
          className="form-control mb-2" 
          type="email" 
          placeholder="Email" 
          value={email} 
          onChange={handleEmailChange} 
        />
        <input 
          className="form-control mb-2" 
          type="password" 
          placeholder="Password" 
          value={password} 
          onChange={handlePasswordChange} 
        />
        <button className="btn btn-primary" onClick={registerUser}>Register</button>
      </div>
    );
  };
  
  export default RegistrationForm;
  