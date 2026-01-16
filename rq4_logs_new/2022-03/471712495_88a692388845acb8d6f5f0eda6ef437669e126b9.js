import React, { useState } from "react";
import Input from "../Input";
import InputAdornment from "@material-ui/core/InputAdornment";
import MailOutlineIcon from "@material-ui/icons/MailOutline";
import "./Login.css";
import Visibility from "@material-ui/icons/Visibility";
import VisibilityOff from "@material-ui/icons/VisibilityOff";
import IconButton from "@material-ui/core/IconButton";
import CustomCheckbox from "../Input/Checkbox";
import image from "../../asset/image/Group.png";
import Buttontype from "@material-ui/core/Button";
import { Link } from "react-router-dom";
import axios from "axios";
//import { API_BASE_URL } from "../../utils/enum";
//import { useSelector, useDispatch } from "react-redux";
//import { setCurrentUser } from "../../Redux/Actions/Auth/action";
//import useApi from "../../utils/useApi";

function Login(props) {
  const [values, setValues] = React.useState({
    password: "",
    Email: "",
    showPassword: false,
  });
  const [error, setError] = useState("");
  //const dispatch = useDispatch();

  //   const auth = useSelector((state) => state.auth);

  //   console.log("auth", auth);

  const emailRegex =
    /^[a-zA-Z0-9.!#$%&'*+/=?^_~{|}`-]+@[a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)*$/;

  const handleChange = (prop) => (e) => {
    setValues({ ...values, [prop]: e.target.value });
  };

  const handleClickShowPassword = () => {
    setValues({ ...values, showPassword: !values.showPassword });
  };

  const handleMouseDownPassword = (e) => {
    e.preventDefault();
  };

  const handleLogin = () => {
    if (!values.Email.match(emailRegex)) {
      console.log("erro");
      setError("Invalid Email");
    } else {
      const formatData = {
        username: values.Email,
        password: values.password,
      };
      //   axios
      //     .post(API_BASE_URL + `auth/signin`, formatData)
      //     .then((res) => {
      //       console.log(res);
      //       const data = {
      //         token: res.data.accessToken,
      //         user: {
      //           name: res.data.username,
      //           role: res.data.roles,
      //         },
      //         isLoggedin: true,
      //       };
      //       localStorage.setItem("meetx-token", res.data.accessToken);
      //       dispatch(setCurrentUser(data));
      //       props.history.push({
      //         pathname: "/",
      //       });
      //     })
      //     .catch((err) => {
      //       console.log(err.response?.data?.error);
      //       setError(err.response?.data?.error);
      //     });
    }
  };

  return (
    <div className="login-main">
      <div className="login-text">
        {/* <div className="meet">
          Meet<span>X</span>
        </div>
        <div className="why">Why MeetX</div> */}
      </div>
      <div className="login-in">
        <div className="login-in1">
          <div>
            <img src={image} alt="logo" />
          </div>
        </div>
        <div className="login-in2">
          <div className="tittle">Login</div>
          <div>
            <div className="input-name">Email</div>
            <div className="int">
              <Input
                placeholder="Enter your corporate email id"
                className="input"
                value={values.Email}
                onChange={handleChange("Email")}
                InputProps={{
                  endAdornment: (
                    <InputAdornment position="end">
                      <MailOutlineIcon className="op" />
                    </InputAdornment>
                  ),
                }}
              />
            </div>
          </div>
          <div>
            <div className="input-name">Password</div>
            <div className="int">
              <Input
                placeholder="Enter your password"
                className="input"
                type={values.showPassword ? "text" : "password"}
                value={values.password}
                onChange={handleChange("password")}
                InputProps={{
                  endAdornment: (
                    <InputAdornment position="end">
                      <IconButton
                        aria-label="toggle password visibility"
                        onClick={handleClickShowPassword}
                        onMouseDown={handleMouseDownPassword}
                      >
                        {values.showPassword ? (
                          <Visibility />
                        ) : (
                          <VisibilityOff />
                        )}
                      </IconButton>
                    </InputAdornment>
                  ),
                }}
              />
            </div>
          </div>
          {error && <div className="warning">{error}</div>}
          <div className="check-main">
            <div className="check">
              <div>
                <CustomCheckbox
                  defaultChecked
                  style={{ color: "#0D1FFF" }}
                  inputProps={{ "aria-label": "secondary checkbox" }}
                />
              </div>
              <div className="gray">Remember me</div>
            </div>
            {/* <div className="forgot">Forgot password ?</div> */}
          </div>
          <div className="login-btn">
            <div className="buton">
              <Buttontype onClick={handleLogin}>Log in</Buttontype>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default Login;