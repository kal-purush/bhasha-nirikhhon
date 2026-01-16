import React, { Component } from 'react';
import { Image } from 'react-native';
import { connect } from 'react-redux';
import { userProfileCreate } from '../actions';
import { Container, Content, Header, Footer, FooterTab, Text, Card, CardItem, Left, Body, Icon, Thumbnail, Right, Form, Item, Input, Label } from 'native-base';
import { Col, Row, Grid } from 'react-native-easy-grid';
import { Button } from './common';
import UserInfoForm from './UserInfoForm';

class UserInfo extends Component {
  render() {
    return (
      <Container>
        <Content>
          <Card>
            <CardItem>
              <UserInfoForm {...this.props} />
            </CardItem>
          </Card>
        </Content>
      </Container>
    );
  }
}

const mapStateToProps = (state) => {
  const { name, phone, bio } = state.userProfileInfo;

  return { name, phone, bio };
};

export default connect(mapStateToProps, { userProfileCreate })(UserInfo);