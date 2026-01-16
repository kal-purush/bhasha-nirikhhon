import React, { Component } from 'react';
import { Redirect } from 'react-router';
import { Container, Form, FormGroup, Label, Input } from 'reactstrap';
import { Button } from 'reactstrap';
import './style.css';

class CatEdit extends Component {
  constructor(props) {
    super(props);
    this.state = {
      form: {
        name: this.props.name,
        age: this.props.age,
        enjoys: this.props.enjoys,
        img: this.props.img
      },
      submitted: false
    };
  }

  handleChange = (e) => {
    let { form } = this.state;
    form[e.target.name] = e.target.value;
    this.setState({ form: form });
  };

  handleSubmit = (e) => {
    e.preventDefault();
    this.props.updateCat(this.state.form, this.props.cat.id);
    this.setState({ submitted: true });
  };

  render() {
    const { name, age, enjoys, img } = this.state.form;
    const { cat } = this.props;
    return (
      <>
        <Container fluid className="cat-form-container">
          <h2>Update Frisky Feline's Profile</h2>
          <Form>
            <FormGroup>
              <Label for="name">Cat's Name</Label>
              <Input
                type="text"
                name="name"
                value={name}
                placeholder={cat.name}
                onChange={this.handleChange}
              />
            </FormGroup>
            <FormGroup>
              <Label for="age">Cat's Age</Label>
              <Input
                type="number"
                name="age"
                value={age}
                placeholder={cat.age}
                onChange={this.handleChange}
              />
            </FormGroup>
            <FormGroup>
              <Label for="enjoys">What does your cat enjoy?</Label>
              <Input
                type="text"
                name="enjoys"
                value={enjoys}
                placeholder={cat.enjoys}
                onChange={this.handleChange}
              />
            </FormGroup>
            <FormGroup>
              <Label for="img">Image link for you fine feline</Label>
              <Input
                type="text"
                name="img"
                value={img}
                placeholder={cat.img}
                onChange={this.handleChange}
              />
            </FormGroup>
            <br />
            <Button onClick={this.handleSubmit}>Edit Cat</Button>
          </Form>
        </Container>
        {this.state.submitted && (
          <Redirect to={`/catshow/${this.props.cat.id}`} />
        )}
      </>
    );
  }
}
export default CatEdit;