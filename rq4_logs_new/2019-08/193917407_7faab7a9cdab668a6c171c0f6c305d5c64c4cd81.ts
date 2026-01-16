import React, { Component, FormEvent } from "react";
import { EmailCrawler as EmailComponent } from './component';
import { Messages } from "../../../validators/types";

export interface EmailState {
  email: string;
  valid: boolean;
}

export interface EmailProps {
  labels: {
    input: string,
    button: string
  },
  onSubmit: (email: string) => void,
  messages: Messages
}

export class EmailContainer extends Component<EmailProps, EmailState> {
  state: EmailState;

  constructor(props: EmailProps) {
    super(props);

    this.state = {
      email: '',
      valid: false
    };
  }

  handleChange = (e: FormEvent<HTMLInputElement | HTMLSelectElement>) => {
    this.setState({ email: e.currentTarget.value });
  };

  handleSubmit = () => {
    this.props.onSubmit(this.state.email);
  };

  handleEmailValid = (valid: boolean ) => {
    this.setState({ valid })
  };

  render() {
    const { labels, messages } = this.props;
    const props = {
      labels,
      value: this.state.email,
      onSubmit: this.handleSubmit,
      onChange: this.handleChange,
      messages,
      valid: this.state.valid,
      onEmailValid: this.handleEmailValid
    };

    return React.createElement(EmailComponent, props);
  }
}