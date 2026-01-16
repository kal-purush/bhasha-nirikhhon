import React, { Component } from "react";
import styled from "styled-components";

const Wrapper = styled.div`
  position: relative;
`;

const StyledInput = styled.input`
  visibility: hidden;
  position: absolute;
`;

class ImageUploader extends Component {
  inputRef = React.createRef();
  reader = new FileReader();

  constructor(props, ...args) {
    super(props, ...args);
    this.reader.onload = this.handleImageLoad;
  }

  handleImageLoad = e => {
    const src = e.target.result;
    this.props.onLoad(src);
  };

  handleImageChange = e => {
    const file = e.target.files[0];

    if (!file.type.startsWith("image/")) {
      return;
    }

    this.reader.readAsDataURL(file);
  };

  handleSubmit = e => {
    e.preventDefault();
    this.inputRef.current.click();
  };

  render() {
    const { renderButton } = this.props;
    return (
      <Wrapper>
        <StyledInput
          type="file"
          innerRef={this.inputRef}
          onChange={this.handleImageChange}
          accept="image/*"
        />
        {renderButton(this.handleSubmit)}
      </Wrapper>
    );
  }
}

export default ImageUploader;