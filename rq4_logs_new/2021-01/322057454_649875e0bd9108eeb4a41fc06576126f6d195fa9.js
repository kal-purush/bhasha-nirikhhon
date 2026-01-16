import React, {useContext, useEffect, useState} from "react";
import Button from "react-bootstrap/Button";
import Modal from "react-bootstrap/Modal";
import Form from "react-bootstrap/Form";
import useFetch from "../../hooks/useFetch";
import {CurrentUserContext} from "../../contexts/currentUser";
import useForm from "../../hooks/useForm";
import './authForm.css'



export default function AuthForm2() {
    const [state, setState] = useContext(CurrentUserContext)
    const [isLoginState, setIsLoginState] = useState(true)
    const descriptionText = isLoginState ? 'Need an account?' : 'Have an account?'
    const apiUrl = isLoginState ? '/token/' : '/signup'
    const [{isLoading, response}, doFetch] = useFetch(apiUrl)

    const { handleChange, handleSubmit, values, errors } = useForm(
        submit,
        isLoginState
    );

    const [show, setShow] = useState(false);
    const handleClose = () => setShow(false);
    const handleShow = () => setShow(true);

    function submit(){

        const user = isLoginState ?
            {
                "username": values.login,
                "password": values.password
            } :
            {
                "username": values.login,
                "password": values.password,
                "email": values.email,
                "profile": {
                    "phone": values.phone
                }
            };

        doFetch({method: 'POST', body: JSON.stringify(user)})

        // if (isLoginState) {
        //     values.login = ''
        //     values.password = ''
        // } else {
        //     values.login = ''
        //     values.password = ''
        //     values.confirm_password = ''
        //     values.email = ''
        //     values.phone = ''
        // }
    }

    useEffect(() => {
        console.log('asdasd');
        if (response != null) {
            if (typeof (response.access) !== 'undefined') {
                localStorage.setItem('token', response.access)
                setState(state => ({
                    ...state,
                    isLoggedIn: true
                }))
            }
            if (typeof (response.refresh) !== 'undefined') {
                localStorage.setItem('refresh', response.refresh)
            }
        }
    }, [response])

    return (
        <>
            <Button variant="outline-secondary" onClick={handleShow}>
                {isLoginState ? 'Sign In++' : 'Sign Up++'}
            </Button>

            <Modal
                show={show}
                onHide={handleClose}
                backdrop="static"
                keyboard={false}
            >
                <Modal.Header closeButton>
                    <Modal.Title>{isLoginState ? 'Sign In' : 'Sign Up'}{' '}
                        <Button variant="link"
                                onClick={() => setIsLoginState(!state)
                                }>
                            {descriptionText}
                        </Button>
                    </Modal.Title>
                </Modal.Header>
                <Modal.Body>
                    <Form>
                        <Form.Group controlId="formBasicLogin">
                            <Form.Label>Логин</Form.Label>
                            <Form.Control type="login"
                                          name="login"
                                          placeholder="Логин"
                                          value={values.login}
                                          onChange={handleChange}
                                          className={`${errors.login && "inputError"}`}

                            />
                            <Form.Text className="text-muted">
                                {errors.login && <p className="error">{errors.login}</p>}
                            </Form.Text>
                        </Form.Group>
                        <Form.Group controlId="formBasicPassword">
                            <Form.Label>Пароль</Form.Label>
                            <Form.Control type="password"
                                          name='password'
                                          placeholder="Пароль"
                                          value={values.password}
                                          onChange={handleChange}
                                          className={`${errors.password && "inputError"}`}

                            />
                            <Form.Text className="text-muted">
                                {errors.password && <p className="error">{errors.password}</p>}
                            </Form.Text>
                        </Form.Group>
                        {
                            !isLoginState &&
                            <Form.Group controlId="formBasicPasswordConfirm">
                                <Form.Label>Подтверждение пароля</Form.Label>
                                <Form.Control type="password"
                                              name="confirm_password"
                                              placeholder="Подтверждение пароля"
                                              value={values.confirm_password}
                                              onChange={handleChange}
                                              className={`${errors.confirm_password && "inputError"}`}

                                />
                                <Form.Text className="text-muted">
                                    {errors.confirm_password && (
                                        <p className="error">{errors.confirm_password}</p>
                                    )}
                                </Form.Text>
                            </Form.Group>
                        }
                        {
                            !isLoginState &&
                            <Form.Group controlId="formBasicEmail">
                                <Form.Label>Email address</Form.Label>
                                <Form.Control type="email"
                                              name='email'
                                              placeholder="Enter email"
                                              value={values.email}
                                              onChange={handleChange}
                                              className={`${errors.email && "inputError"}`}

                                />
                                <Form.Text className="text-muted">
                                    {errors.email && <p className="error">{errors.email}</p>}
                                </Form.Text>
                            </Form.Group>
                        }
                        {
                            !isLoginState &&
                            <Form.Group controlId="formBasicPhone">
                                <Form.Label>Номер Телефона</Form.Label>
                                <Form.Control type="phone"
                                              name='phone'
                                              placeholder="Enter phone"
                                              value={values.phone}
                                              onChange={handleChange}
                                              className={`${errors.phone && "inputError"}`}

                                />
                                <Form.Text className="text-muted">
                                    {errors.phone && <p className="error">{errors.phone}</p>}
                                </Form.Text>
                            </Form.Group>
                        }
                    </Form>
                </Modal.Body>
                <Modal.Footer>
                    <Button variant="secondary" onClick={handleClose}>
                        Close
                    </Button>
                    <button className="btn btn-primary"
                            type="submit"
                            onClick={handleSubmit}
                    >
                        {
                            isLoading ?
                                <span className="spinner-border spinner-border-sm" role="status"
                                      aria-hidden="true"> </span>
                                : ''
                        }
                        {isLoading ? 'Loading...' : 'Отправить'}
                    </button>
                </Modal.Footer>
            </Modal>
        </>
    );
}