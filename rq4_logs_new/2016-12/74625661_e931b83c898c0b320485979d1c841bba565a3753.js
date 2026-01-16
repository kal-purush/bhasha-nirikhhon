'use strict'
import React, { Component } from 'react'
import { Button, Form, Input, Message, Segment, Divider } from 'semantic-ui-react'
import { withRouter } from 'react-router';
import base from 'config/base'


class SignUpForm extends Component {
  static contextTypes = { router: React.PropTypes.object }

  state = { serializedForm: {} }
  handleChange = (e, { value }) => this.setState({ value })

  authHandler = (error, user) => {
    if (error) console.log('errored: ' ,error.message)
    else this.context.router.push('/feed')
  }

  login = (serializedForm) => base.authWithPassword(serializedForm, this.authHandler);

  handleSignUp = (e, serializedForm) => {
    e.preventDefault()
    this.setState({ serializedForm })
    this.login(serializedForm)
  }

  render() {
    const { serializedForm, value } = this.state
    return (
      <Form onSubmit={this.handleSignUp}>
        <Form.Input label='Email' name='email' type='email' />
        <Form.Input label='Password' name='password' type='password' />
        <Segment padded>
          <Button fluid color='yellow' type='submit' value='submit'>SignUp</Button>
          <Divider horizontal>Or</Divider>
          <Button fluid secondary onClick={(e)=>{this.context.router.push('/signup')}}>Sign Up</Button>
        </Segment>
        <Message>
          <pre>serializedForm: {JSON.stringify(serializedForm, null, 2)}</pre>
        </Message>
      </Form>
    )
  }
}

export default withRouter(SignUpForm)