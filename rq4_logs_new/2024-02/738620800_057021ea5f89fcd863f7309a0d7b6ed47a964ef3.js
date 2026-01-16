import { useState, useEffect } from "react";
import { useNavigate, Link } from "react-router-dom";
import { useDispatch, useSelector } from "react-redux";
import { Form, Button, Row, Col } from "react-bootstrap";
import { toast } from "react-toastify";

import FormContainer from "../components/FormContainer";
import InputField from "../components/InputField";
import Loader from "../components/Loader";

import { setAuthetnication } from "../slices/authSlice";
import { useLoginMutation } from "../slices/api/authApiSlice";

import useRedirect from "../hooks/useRedirect";

const LoginPage = () => {
    const [email, setEmail] = useState("");
    const [password, setPassword] = useState("");

    const dispatch = useDispatch();
    const navigate = useNavigate();

    const { isAuthenticated } = useSelector(state => state.auth);
    const [login, { isLoading }] = useLoginMutation();
    const redirect = useRedirect();

    useEffect(() => {
        if (isAuthenticated) {
            navigate(redirect, { replace: true });
        }
    }, [isAuthenticated, navigate, redirect]);

    const loginSubmitHandler = async (event) => {
        event.preventDefault();

        try {
            const userProfile = await login({ email, password }).unwrap();
            dispatch(setAuthetnication({ ...userProfile }));
            navigate(redirect, { replace: true });
        } catch (error) {
            toast.error(error?.data?.message || error?.error);
        }
    };

    return (
        <FormContainer>
            <h1>Sign In</h1>

            <Form onSubmit={loginSubmitHandler}>
                <InputField
                    label={"Email Address"}
                    controlId={"email"}
                    type={"email"}
                    placeholder={"Enter your email ..."}
                    value={email}
                    changeValueHandler={(event) => setEmail(event.target.value)}
                />
                <InputField
                    label={"Password"}
                    controlId={"password"}
                    type={"password"}
                    placeholder={"Enter your password ..."}
                    value={password}
                    changeValueHandler={(event) => setPassword(event.target.value)}
                />

                <Button
                    type="submit"
                    variant="dark"
                    disabled={isLoading}
                >
                    Sign In
                </Button>
            </Form>

            {isLoading && <Loader />}

            <Row className="py-3">
                <Col>
                    New Customer?{" "}
                    <Link to={`/register?redirect=${redirect}`}>
                        Register
                    </Link>
                </Col>
            </Row>
        </FormContainer>
    );
};

export default LoginPage;