import React, { useState } from 'react';
import Container from 'react-bootstrap/Container';
import Button from 'react-bootstrap/Button';
import Form from 'react-bootstrap/Form';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import { motion } from "framer-motion";
import { useSelector, useDispatch } from 'react-redux';
import Spinner from '../Spinner/Spiner'
import Footer from '../Home/Footer/Footer';
import { resetPassword } from '../../../store/auth-slice';
import './ForgotPassword.css';



const isEmpty = value => value.trim() === ''
const isPwdAndCnfSame = (pwd, cnf) => pwd === cnf



// Animation for moving the component from left to right
const containerVariants = {
    hidden: {
        x: 400,
        opacity: 0
    },
    visible: {
        x: 0,
        opacity: 1,
    }
}



export default function ForgotPassword() {

    const dispatch = useDispatch();

    const { isError, isSuccess, isLoading, message } = useSelector((state) => state.authReducer);

    const [resetPasswordData, setResetPasswordData] = useState({
        token:'',
        password: '',
        confirmpassword: ''
    });


    const [resetPasswordFormValidity, setResetPasswordFormValidity] = useState({
        token:true,
        password: true,
        confirmpassword: true
    });


    const handleRegisterData = (e) => {
        setResetPasswordData((prevState) => {
            return {
                ...prevState,
                [e.target.id]: e.target.value
            }
        })
    }


    const handleSubmit = (e) => {
        e.preventDefault();


        // Checking the Validity
        const isTokenValid = !isEmpty(resetPasswordData.token);
        const isPasswordValid = !isEmpty(resetPasswordData.password);
        const isConfirmPasswordValid = isPwdAndCnfSame(resetPasswordData.password, resetPasswordData.confirmpassword);


        // Setting the form Validity and accordingly validation error will be shown for the fields
        setResetPasswordFormValidity({
            token: isTokenValid,
            password: isPasswordValid,
            confirmpassword: isConfirmPasswordValid
        });


        const isFormValid = isTokenValid && isPasswordValid && isConfirmPasswordValid;


        // If the form not valid the control will be removed from the flow
        if (!isFormValid) {
            return;
        }


        // If everything is valid then the API will be called with the registerData paramter
        dispatch(resetPassword(resetPasswordData));
    }

    return (
        <>

            {/* During loading spinner over the backdrop */}
            {isLoading && <div id="backdropRegister">
                <Spinner />
            </div>}

            <motion.div
                variants={containerVariants}
                initial="hidden"
                animate="visible"
            >
                <Container >


                    {/* Success Messages coming from the api will be shown in Bootstrap 5 alert */}
                    {isSuccess && <Row>
                        <Col md={6} className="m-auto mb-3">
                            <div className="alert alert-success text-center" role="alert">
                                <b>{message}</b>
                            </div>
                        </Col>
                    </Row>}

                    
                    {/* Error Message coming from the api will be shown in Bootstrap 5 alert */}
                    {isError && <Row>
                        <Col md={6} className="m-auto mb-3">
                            <div className="alert alert-danger text-center" role="alert">
                                <b>{message}</b>
                            </div>
                        </Col>
                    </Row>}


                    <Row>
                        <Col md={6} className="m-auto p-3" style={{ borderRadius: "20px" }}>
                            <h2 id="register" className="text-center mb-3">Update password</h2>
                            <Form onSubmit={handleSubmit}>

                                <motion.div
                                    whileHover={{ scale: 1.1 }}

                                    className="mb-5"
                                >
                                    <Form.Group className="form-group">
                                        <span className="material-symbols-outlined b-0 p-2 colouring" >
                                            token
                                        </span>
                                        <input id="token" type="text" autoComplete="on" placeholder="Enter the token from email" className="colouring" onChange={handleRegisterData} />
                                    </Form.Group>
                                    <div>
                                        {!resetPasswordFormValidity.token && <p className="text-danger text-end">**Please enter the token</p>}
                                    </div>
                                </motion.div>



                                <motion.div
                                    whileHover={{ scale: 1.1 }}

                                    className="mb-5"
                                >
                                    <Form.Group className="form-group">
                                        <span className="material-symbols-rounded b-0 p-2 colouring">
                                            lock
                                        </span>
                                        <input id="password" type="password" autoComplete="on" placeholder="Enter the password" className="colouring" onChange={handleRegisterData} />
                                    </Form.Group>
                                    <div>
                                        {!resetPasswordFormValidity.password && <p className="text-danger text-end">**Please enter the password correctly</p>}
                                    </div>
                                </motion.div>



                                <motion.div
                                    whileHover={{ scale: 1.1 }}

                                    className="mb-5"
                                >
                                    <Form.Group className="form-group">
                                        <span className="material-symbols-rounded b-0 p-2 colouring">
                                            <span className="material-symbols-outlined">
                                                lock
                                            </span>
                                        </span>
                                        <input id="confirmpassword" type="password" autoComplete="on" placeholder="Confirm password" className="colouring" onChange={handleRegisterData} />
                                    </Form.Group>

                                    <div>
                                        {!resetPasswordFormValidity.confirmpassword && <p className="text-danger text-end">**Confirm Password and Password should be same</p>}
                                    </div>

                                </motion.div>



                                <motion.div
                                    whileHover={{ scale: 1.1 }}
                                    whileTap={{ scale: 0.9 }}
                                    id="btn">
                                    <Button className="m-auto" id="forgot-password-button" type="submit">
                                        Update Password
                                    </Button>
                                </motion.div>

                                
                            </Form>
                        </Col>
                    </Row>
                </Container>
            </motion.div>


            <Footer />
        </>

    )
}