import React from "react";
import styled from "styled-components";
import Avatar from "@material-ui/core/Avatar";
import IconButton from "@material-ui/core/IconButton";
import {
  FaComment,
  FaAngleDoubleRight,
  FaQuoteLeft,
  FaQuoteRight,
  FaTrash
} from "react-icons/fa";
import { CSSTransition, TransitionGroup } from "react-transition-group";
import ButtonBase from "@material-ui/core/ButtonBase";
import uuidv4 from "uuid/v4";

function getID() {
  return uuidv4().replace(/\-/g, "");
}

let data = {
  user: {
    id: "85eb742cb7664a3895fac3ea6ad5993b",
    name: "Paddington",
    avatarURL: "https://avatarfiles.alphacoders.com/126/126546.jpg",
    recipes: {}
  },
  comments: [
    {
      id: getID(),
      message: "Aunt Lucy's original recipe! It's incredible!",
      avatarURL: "https://avatarfiles.alphacoders.com/126/126546.jpg",
      timestamp: Date.now()
    }
  ]
};

const Form = styled.form`
  display: flex;
`;

const TextInput = styled.textarea`
  font-size: 16px;
  resize: none;
  border: none;
  width: 100%;
  &:focus {
    outline: none;
  }
`;

const Container = styled.div`
  width: 100%;
  max-width: 525px;
`;

const Title = styled.h5`
  margin: 0 0 0.5rem 0;
  padding: 0.5rem;
`;

const StyledAvatar = styled(Avatar)`
  margin: 0;
  padding: 0;
  width: ${props => (props.width ? props.width : "40px")};
  height: ${props => (props.height ? props.height : "40px")};
`;

const Button = styled(ButtonBase)`
  text-transform: uppercase;
  &:disabled {
    color: grey;
  }
`;

function UserAvatar({ src, alt, width = "40px", height = "40px" }) {
  return (
    <IconButton
      style={{
        margin: ".5rem",
        padding: 0,
        width: "min-content",
        height: "min-content"
      }}
    >
      <StyledAvatar src={src} alt={alt} width={width} height={height} />
    </IconButton>
  );
}

function Comment({ message, avatarURL, timestamp, ...rest }) {
  return (
    <div {...rest} style={{ display: "flex", margin: ".5rem" }}>
      <div
        style={{
          borderRight: "1px solid black",
          display: "flex",
          justifyContent: "center",
          alignItems: "center"
        }}
      >
        <UserAvatar
          src={data.user.avatarURL}
          alt="profile image"
          width="20px"
          height="20px"
        />
      </div>
      <div
        style={{
          display: "flex",
          flexDirection: "column",
          width: "100%"
        }}
      >
        <div style={{ display: "flex", flexWrap: "wrap", padding: "0 .5rem" }}>
          <p>{message}</p>
        </div>
        <div
          style={{
            width: "100%",
            display: "flex",
            flexDirection: "column",
            justifyContent: "center",
            alignItems: "flex-end"
          }}
        >
          <span style={{ fontSize: "10px" }}>{`- ${"Paddington"}`}</span>
          <em style={{ fontSize: "8px", color: "grey" }}>
            {new Date(timestamp).toLocaleDateString()}
          </em>
        </div>
      </div>
    </div>
  );
}

class Comments extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      message: "",
      showInput: false,
      showButton: true
    };
    this.fontAwesomeStyles = { marginLeft: ".25em", verticalAlign: "middle" };
  }
  toggleInput = () =>
    this.setState(({ showInput, showButton }) => ({
      showInput: !showInput,
      showButton: !showButton
    }));

  onTextChange = ({ target }) => this.setState({ [target.id]: target.value });

  onSubmit = event => {
    event.preventDefault();
    const message = event.target.elements.message.value;
    const comment = {
      message,
      id: getID(),
      timestamp: Date.now(),
      avatarURL: "https://avatarfiles.alphacoders.com/126/126546.jpg"
    };

    data = {
      ...data,
      comments: data.comments.concat([comment])
    };

    this.forceUpdate();
    this.setState({ message: "" });
  };

  clearField = () => {
    if (this.state.message !== "") {
      if (window.confirm("Clear comment?"));
      else {
        return;
      }
    }
    this.setState({ message: "", showInput: false });
  };

  fieldDisabled = () => this.state.message === "";

  handleClick = event => {};
  render() {
    return (
      <Container>
        <Title>Responses</Title>
        <TransitionGroup>
          {data.comments.map(comment => (
            <CSSTransition
              key={comment.id}
              classNames="slide-up"
              timeout={300}
              unmountOnExit
            >
              <Comment key={comment.id} {...comment} />
            </CSSTransition>
          ))}
        </TransitionGroup>
        <CSSTransition
          timeout={300}
          classNames="slide-in"
          in={this.state.showButton}
          unmountOnExit
        >
          <Button
            onClick={this.toggleInput}
            style={{ float: "right", fontSize: 13 }}
          >
            Comment
            <FaComment style={this.fontAwesomeStyles} />
          </Button>
        </CSSTransition>
        <CSSTransition
          in={this.state.showInput}
          classNames="fade-in"
          timeout={300}
          onEnter={() => this.input.focus()}
          onExited={() => this.setState({ showButton: true })}
          unmountOnExit
        >
          <div style={{ margin: "1em 0" }}>
            <Title>Your Comment</Title>
            <Form id="comment-form" onSubmit={this.onSubmit}>
              <UserAvatar src={data.user.avatarURL} alt={"Profile"} />
              <TextInput
                placeholder="What a wonderful recipe!"
                id="message"
                type="text"
                value={this.state.message}
                onChange={this.onTextChange}
                innerRef={input => (this.input = input)}
              />
            </Form>

            <Button
              style={{ float: "right", margin: ".25rem", fontSize: 13 }}
              form="comment-form"
              type="submit"
              disabled={this.fieldDisabled()}
            >
              Publish
              <FaAngleDoubleRight style={this.fontAwesomeStyles} />
            </Button>
            <Button
              style={{ float: "right", margin: ".25rem", fontSize: 13 }}
              form="comment-form"
              onClick={this.clearField}
            >
              Cancel
              <FaTrash style={this.fontAwesomeStyles} />
            </Button>
          </div>
        </CSSTransition>
      </Container>
    );
  }
}

export default Comments;