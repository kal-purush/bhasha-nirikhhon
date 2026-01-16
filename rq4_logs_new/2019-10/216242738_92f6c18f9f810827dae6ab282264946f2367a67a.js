import React from "react";
import "./style.css";
import { Container, Row, Col } from "react-grid-system";

function RumbleCats(props) {
  return (
    <Container>
      <Row>
        <Col>
          <div className="card">
            <h5 className="card-subtitle">{props.alias}</h5>
            <div
              //Need a new prop - isClicked - boolean - used to determine which cat will play the game
              onClick={() => props.clicked(props.isClicked, props.id)}
              className="img-container"
            >
              <img className="cat-pic" src={props.image} />
            </div>
          </div>
          <div className="versus">VS</div>
          <div className="card">
            <h5 className="card-subtitle">{props.alias}</h5>
            <div
              //Need a new prop - isClicked - boolean - used to determine which cat will play the game
              onClick={() => props.clicked(props.isClicked, props.id)}
              className="img-container"
            >
              <img className="cat-pic" src={props.image} />
            </div>
          </div>
        </Col>
      </Row>
    </Container>
  );
}

export default RumbleCats;