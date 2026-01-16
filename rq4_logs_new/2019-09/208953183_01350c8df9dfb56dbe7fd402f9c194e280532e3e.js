import React from 'react';
import {Container, Col, Row, Form, Button} from 'react-bootstrap'

class Login extends React.Component {

    constructor (props){
        super(props);

        this.handleSubmit = this.handleSubmit.bind(this);
    }
    handleSubmit(e) {
        e.preventDefault();
        e.stopPropagation();
        this.props.route.history.push('/home');
    }

    render() {
        return (
            <Container>
                <Row>
                    <Col xs={12} sm={4}>
                        <Form onSubmit={(e)=> this.handleSubmit(e)}>
                            <Form.Group controlId="formBasicEmail">
                                <Form.Label>Email address</Form.Label>
                                <Form.Control type="email" placeholder="Enter email" />
                                <Form.Text className="text-muted">
                                We'll never share your email with anyone else.
                                </Form.Text>
                            </Form.Group>

                            <Form.Group controlId="formBasicPassword">
                                <Form.Label>Password</Form.Label>
                                <Form.Control type="password" placeholder="Password" />
                            </Form.Group>
                            <Form.Group controlId="formBasicCheckbox">
                                <Form.Check type="checkbox" label="Check me out" />
                            </Form.Group>
                            <Button variant="primary" type="submit">
                                Submit
                            </Button>
                        </Form>
                    </Col>
                </Row>
            </Container>              
        )
    }
}
export default Login