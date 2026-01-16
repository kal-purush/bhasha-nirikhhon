import React from 'react';
import { connect } from 'react-redux';
import {
  Card,
  CardBody,
  CardHeader,
  CardImg,
  Col,
  Row,
  Button,
} from 'reactstrap';

import './Methodology.css';

import SkillsList from './SkillsList';

const MethodologyList = props => {
  const { skillsListContent } = props.skillsList;

  const createMethodologyList = skillsListContent.map(methodology => {
    return (
      <Col key={methodology.key}>
        <div className="methodology ui-container">
          <Card>
            <CardHeader>{methodology.topic}</CardHeader>
            <CardImg src={methodology.image} />
            <CardBody>
              {methodology.stack.map(lang => {
                return <div key={lang.key}>{lang.language}</div>;
              })}
            </CardBody>
            <Button color="secondary">Button</Button>
          </Card>
        </div>
      </Col>
    );
  });

  return (
    <div>
      <Row>{createMethodologyList}</Row>
    </div>
  );
};

const mapStateToProps = state => {
  return { skillsList: state.loadSkillsListContent };
};

export default connect(mapStateToProps)(MethodologyList);