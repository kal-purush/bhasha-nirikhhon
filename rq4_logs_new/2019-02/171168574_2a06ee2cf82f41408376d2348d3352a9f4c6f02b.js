// The 404 error page component
import React from "react";
// import the grid component
import { Col, Row, Container } from "../components/Grid";
// import the jumbotron component
import Jumbotron from "../components/Jumbotron";

function NoMatch() {
  return (
    <Container>
      <Row>
        <Col size = "md-12">
          <Jumbotron>
            <h1>404 Page Not Found.</h1>
          </Jumbotron>
        </Col>
      </Row>
    </Container>
  );
}

export default NoMatch;