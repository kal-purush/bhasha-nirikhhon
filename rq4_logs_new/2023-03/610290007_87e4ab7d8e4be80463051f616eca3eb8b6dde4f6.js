import React from "react";
import "bootstrap/dist/css/bootstrap.min.css";
import "bootstrap/dist/js/bootstrap.bundle.min";
import Container from 'react-bootstrap/Container';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import axios from 'axios';

import { Dashboard } from "./modules/dashboard";

let refCount = 0;

function setLoading(isLoading) {
    if (isLoading) {
        refCount++;
        document.getElementById('loader').style = 'display: block';
    } else if (refCount > 0) {
        refCount--;
        if (refCount > 0) document.getElementById('loader').style = 'display: block';
        else {
            // added delay for loader demo
            setTimeout(() => {
                document.getElementById('loader').style = 'display: none';
            }, 500);
        }
    }
}

axios.interceptors.request.use(config => {
    setLoading(true);
    return config;
}, error => {
    setLoading(false);
    return Promise.reject(error);
});

axios.interceptors.response.use(response => {
    setLoading(false);
    return response;
}, error => {
    setLoading(false);
    return Promise.reject(error);
});

axios.interceptors.response.use(
    response => response,
    error => {
        const { loggedInUser } = store.getState().userReducer;

        if (error.response && error.response.status === 401 && loggedInUser) window.location = "/login";

        return Promise.reject(error);
    }
);

const App = () => {
    return (
        <Container>
            <Row className="header">
                <Col className="p-3">HEADER</Col>
            </Row>
            <Row>
                <Col className="side-panel py-4">
                    All the Pokémon data you'll ever need in one place,
                    easily accessible through a modern RESTful API.
                </Col>
                <Col xs={10}>
                    <Dashboard />
                </Col>
            </Row>
            <Row className="p-3 footer">
                <Col>FOOTER</Col>
            </Row>
        </Container>
    );
};

export default App;