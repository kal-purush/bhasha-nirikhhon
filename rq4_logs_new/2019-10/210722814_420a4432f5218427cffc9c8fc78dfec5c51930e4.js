import React from "react";
import { Card, CardTitle, Button, CardBody, UncontrolledCollapse, Container, Row, Col } from 'reactstrap';


function VideoCardCreator(props) {


    return (
        <div className="videoContainer">

            <Container className="buttonContent">
                <Row>
                    <Col>
                        <CardTitle style={{color: 'white'}}>Title: {props.image.title}</CardTitle>
                    </Col>
                    <Col>
                        <Button color="secondary" id="toggler" style={{ padding: '1% 3%', margin: '.25% auto'}}>View Desciption
                        </Button>
                    </Col>
                    <Col style={{color: 'white'}}>Date: {props.image.date}</Col>
                </Row>
            </Container>

            <UncontrolledCollapse toggler="#toggler">
                <Card>                
                    <CardBody>
                    {props.image.explanation}
                    </CardBody>
                </Card>

            </UncontrolledCollapse>
            <div className="videoFrame">
                <iframe className="media" width="800" height="450" src={props.image.url} title="NASA Video of the Day" style={{ padding: '3px', border: '5px solid grey', borderRadius: '10px'}} />
            </div>
        </div>
    );
}

export default VideoCardCreator;