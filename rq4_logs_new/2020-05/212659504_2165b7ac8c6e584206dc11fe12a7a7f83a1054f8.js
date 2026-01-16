import React from 'react';
import { Container, Row, Col } from 'react-bootstrap'

class About extends React.Component {
    render() {
        return (
            <Container fluid className="page">
                    <Row className="text-center" style={{ padding: "3vh" }}>
                        <Col>
                            <h1>About me</h1>
                        </Col>
                    </Row>
                    <Row className="justify-content-center">
                        <Col md={6}>
                            <p>I'm a software developer working for <a href="http://www.numerous.app/">Numerous Technology.</a> I have a serious passion for product design, problem solving and creating intelligent, scalable, fast solutions. My hunger for knowledge and determination to turn information into action has contributed to my success at my company, in building exciting new products to quickly impact the target market.</p>
                            <p>I am a chronic puzzle-seeker and a lifelong learner. I unpack complicated problems by approaching each with the flexible process and attention it deserves.</p>

                            <p>This means that I work with others to ask questions, define approaches and execute solutions comprising:</p>
                            <ul>
                                <li>
                                    Strategy
                                </li>
                                <li>
                                    Information architecture
                                </li>
                                <li>
                                    Front and backend web development
                                </li>
                            </ul>
                        </Col>
                    </Row>
            </Container>
        );
    }
}

export default About;