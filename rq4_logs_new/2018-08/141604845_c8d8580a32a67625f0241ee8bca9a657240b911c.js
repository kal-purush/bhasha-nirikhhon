import React, { PureComponent } from 'react';
import PropTypes from 'prop-types';
import styled from 'styled-components';

const StyledButton = styled.button`
  margin: 1em;
  padding: 0.5em 1em;

  background: ${props => (props.primary ? 'indianred' : 'ghostwhite')};
  border: 2px solid indianred;
  border-radius: 3px;

  color: ${props => (props.primary ? 'ghostwhite' : 'indianred')};
  font-size: 1em;
`;

export default class Button extends PureComponent {
  render() {
    return (
      <div>
        <StyledButton primary={this.props.primary}>{this.props.children}</StyledButton>
      </div>
    );
  }
}

Button.propTypes = {
  primary: PropTypes.bool,
  children: PropTypes.string
};