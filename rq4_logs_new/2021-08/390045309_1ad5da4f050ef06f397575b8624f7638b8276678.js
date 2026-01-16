import React, { useContext } from 'react';
import firebase from "firebase/app";
import "firebase/auth";
import firebaseConfig from "../firebase.config";
import { useState } from "react";
import Swal from 'sweetalert2'
import withReactContent from 'sweetalert2-react-content'
import { UserContext } from './../../../App';
import { useHistory, useLocation, Link } from 'react-router-dom';
import Navbar from '../../Home/Navbar/Navbar';
import './Login.css';
import { useForm } from "react-hook-form";
import logo from '../../../images/Asset-6.png';
import google from '../../../images/google.png';


if (!firebase.apps.length) {
    firebase.initializeApp(firebaseConfig);
 }else {
    firebase.app(); // if already initialized, use that one
 }
// firebase.initializeApp(firebaseConfig);
const MySwal = withReactContent(Swal)

const Login = () => {
    const [loggedInUser, setLoggedInUser] = useContext(UserContext);
    let history = useHistory();
    let location = useLocation();
    let { from } = location.state || { from: { pathname: "/" } };

    // const { register, watch, formState: { errors } } = useForm();
    // const onSubmit = data => console.log(data);

    const provider = new firebase.auth.GoogleAuthProvider();

      // Sign In & Sign Out With Google Account
  const [user, setUser] = useState({
    isSignedIn: false,
    name: '',
    photoURL : '',
    email: '',
    password: '',
    error: '',
    success: '',
  })

  const handleSignIn = () => {
    //console.log('Clicked');
    firebase.auth()
      .signInWithPopup(provider)
      .then((result) => {
        const {displayName, photoURL, email} = result.user;
        //console.log(displayName, photoURL, email);
        const signedInUser = {
          isSignedIn: true,
          name: displayName,
          photoURL: photoURL,
          email: email,

        }
        setUser(signedInUser);
        setLoggedInUser(signedInUser);
        history.replace(from);
        MySwal.fire({
            didOpen: () => {
                
              MySwal.clickConfirm()
            }
          }).then(() => {
            return MySwal.fire({
                icon: 'success',
            title: 'Signed in successfully',
            toast: true,
            position: 'bottom-end',
            showConfirmButton: false,
            timer: 3000,
            timerProgressBar: true,
            })
          })
      }).catch((error) => {
        Swal.fire({
            icon: 'error',
            title: 'Oops...',
            text: 'Something went wrong!',
          })
        //console.log(error.message)
  });

  }

  const handleSignOut = () => {
    firebase.auth().signOut().then((response) => {
      // Sign-out successful.
      const signedOutUser = {
        isSignedIn: false,
        name: '',
        photoURL : '',
        email: '',
      }
      setUser(signedOutUser);
      MySwal.fire({
        didOpen: () => {
            
          MySwal.clickConfirm()
        }
      }).then(() => {
        return MySwal.fire({
        icon: 'success',
        title: 'Signed out successfully',
        toast: true,
        position: 'bottom-end',
        showConfirmButton: false,
        timer: 3000,
        timerProgressBar: true,
        })
      })
    }).catch((error) => {
      // An error happened.
    });
  }

  // Sign Up & Sign In With Email Address & Password

  // const [newUser, setNewUser] =useState(false);

  const handleBlur = (event) => {
    let isFieldValid;
    if(event.target.name === 'email'){
      isFieldValid = /\S+@\S+\.\S+/.test(event.target.value);
      //console.log(isFieldValid);
    }
    if(event.target.name === 'password'){
      const isPasswordValid = event.target.value.length > 6;
      const passwordHasNumber = /\d{1}/.test( event.target.value)
      isFieldValid = isPasswordValid && passwordHasNumber;

    }
    if(event.target.name === 'name'){
      isFieldValid = event.target.value;
      //console.log(isFieldValid);
    }
    if(isFieldValid){
     const newUserInfo = {...user};
     newUserInfo[event.target.name] = event.target.value;
     setUser(newUserInfo);

    }
     //console.log(event.target.value);
  }

  const handleSubmit = (event) => {
    //console.log(user.email, user.password)
    // if(newUser && user.email && user.password){
    //    //console.log('submitting')
    //    firebase.auth().createUserWithEmailAndPassword(user.email, user.password)
    //     // Handle Errors here.
    //     .then(response => {
    //       const newUserInfo = {...user};
    //       newUserInfo.error = '';
    //       //console.log(response)
          
    //       updateUserName(user.name);
    //       newUserInfo.success = true;
    //       verificationMail(user.email);
    //       setUser(newUserInfo);
    //     })
    //     .catch(error =>{
    //     const newUserInfo = {...user};
    //     //var errorCode = error.code;
    //     //var errorMessage = error.message;
    //     //console.error(errorCode,errorMessage)
    //     newUserInfo.errorCode = error.code;
    //     newUserInfo.error = error.message;
    //     newUserInfo.success = false;
    //     setUser(newUserInfo);
    //     })
       
        
      
    // }
    
    // if(!newUser && user.email && user.password){
      if(user.email && user.password){
      firebase.auth().signInWithEmailAndPassword(user.email, user.password)
      .then(response => {
        const newUserInfo = {...user};
          newUserInfo.error = '';
          newUserInfo.isSignedIn = true;
          //console.log(response)
          newUserInfo.success = true;
          setUser(newUserInfo);
          setLoggedInUser(newUserInfo);
          history.replace(from)
          //console.log('sign in user info', response.user)
          MySwal.fire({
            didOpen: () => {
                
              MySwal.clickConfirm()
            }
          }).then(() => {
            return MySwal.fire({
            icon: 'success',
            title: 'Signed in successfully',
            toast: true,
            position: 'bottom-end',
            showConfirmButton: false,
            timer: 3000,
            timerProgressBar: true,
            })
          })
      })
      .catch(function(error) {
          //console.log(error);
        const newUserInfo = {...user};
        // var errorCode = error.code;
        // var errorMessage = error.message;
        // console.error(errorCode,errorMessage)
        newUserInfo.errorCode = error.code;
        newUserInfo.error = error.message;
        newUserInfo.success = false;
        setUser(newUserInfo);
        Swal.fire({
            icon: 'error',
            title: `${newUserInfo.errorCode}`,
            text: `${newUserInfo.error}`,
          })
      });
    }

    event.preventDefault()
  }

  // const updateUserName = name => {
  //   const user = firebase.auth().currentUser;

  //     user.updateProfile({
  //       displayName: name,
        
  //     }).then(function() {
  //       // Update successful.
  //       //console.log(username updated successfully)
  //       console.log("User name updated successfully");
  //     }).catch(function(error) {
  //       // An error happened.
  //     });
  // }

  // const verificationMail = (email) => {
  //   firebase.auth().currentUser.sendEmailVerification()
  //   .then(() => {
  //   //   window.alert('Verification email sent');
  //   Swal.fire({
  //       icon: 'info',
  //       title: 'Done',
  //       text: 'Verification Email Sent',
  //     })
  //   });
  // }

    return (
        <div>
            {/* Google Account Sign In */}

      {/* {
        user.isSignedIn ? <button onClick={handleSignOut}>Sign out</button> : <button onClick={handleSignIn}>Sign in</button>
      } */}
      {/* {
        user.isSignedIn && 
        <div>
          <p>Welcome {user.name}</p>
          <p>Email: {user.email}</p>
          <img src={user.photoURL} alt="photo"></img>
        </div>
      } */}


    {/* Sign Up and Sign In With Email Address & Password */}

      {/* <h3>Email & Password Method</h3>
      {
        user.isSignedIn? 
        <div>
          <p>Welcome {user.name}</p>
          <p>Email: {user.email}</p>
        </div>: 
        <div>
            <input type="checkbox" onChange={() => setNewUser(!newUser)} name="newUser" id=""/>
            <label htmlFor="newUser">New User Sign Up</label>
            <form onSubmit={handleSubmit} action="">
            {newUser && <input type="text" name='name' onBlur={handleBlur} placeholder='name' required/>}
            <br/>
            <input type="text" name="email" onBlur={handleBlur} placeholder="write your email address" required/>
            <br/>
            <input type="password" name="password" onBlur={handleBlur} placeholder="your password" required/>
            <br/>
            <input type="submit" value={newUser ? 'Submit' : 'Sign In'}></input>
          </form>
        </div>
      }
      
      <p style={{color: 'red'}}>{user.error}</p>
      {
        user.success && <p style={{color: 'green'}}>User {newUser ? 'created' : 'logged in'} success!</p>
      } */}
        <Navbar></Navbar>
        <div className="login-container p-5">
        <div className="container px-5">
          <div className="row align-items-center login-row">
              <div className="col-md-6 col-sm-10">
                <div className="text-center" style={{color: '#152550'}}>
                  <h2 className="fw-bold mb-4">Hello!<span className="ms-3"></span></h2>
                  <p>Sign in to your account.</p>
                </div>
                <form  onSubmit={handleSubmit} className="login-form p-5">
                            
                            <div className="mb-4">
                                <input name="email" type="text" onBlur={handleBlur} className="form-control login-input" placeholder="E-mail" />
                            </div>
                            <div className="mb-4">
                                <input onBlur={handleBlur} name="password" type="password" className="form-control login-input" placeholder="Password" />
                                
                            </div>
                            <div className="mb-4">
                                <input type="submit" value="Sign In" className="btn btn-order text-white mb-3"></input>
                            </div>
                </form>
                <div className="gContainer-1">
                  <div className="gContainer-2 text-center">
                    <button onClick={handleSignIn}  className="google-btn p-2 d-flex" style={{
                      border: "none",
                      backgroundColor: 'white',
                      color: '#152550',
                      borderRadius: '50px',
                      outline: 'none',
                      boxShadow: '0px 0px 10px #a2edf7'
                      }}>
                      <img style={{
                        height: '25px', 
                        width: '25px', 
                        
                      }} src={google} alt=""/>
                        <h6 className="px-5 pt-1">Continue with Google</h6>
                      </button>
                  </div>
                </div>
                
                <p className="text-center pt-3">Don't have an account?<span className="ps-1"><Link to="/signUp">Create</Link></span></p>
              </div>
              <div className="col-md-6 col-sm-10 coloured-part">
                <div className="pt-5">
                  <div className="p-5 text-center">
                  <img style={{height: '90px', width: '90px'}} src={logo} alt="" />
                    <h1 className="text-center text-white fw-bold">Welcome Back!</h1>
                    <p className="text-center text-white fw-bold">Lorem ipsum dolor sit amet consectetur adipisicing elit. Repellat magni voluptate cupiditate reprehenderit.</p>
                  </div>
                </div>      
              </div>
          </div>
        </div>
        </div>
      </div>
    );
};

export default Login;