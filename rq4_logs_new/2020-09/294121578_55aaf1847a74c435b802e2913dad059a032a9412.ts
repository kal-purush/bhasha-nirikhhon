import React from "react";
import styled from "styled-components";
import Button from "@material-ui/core/Button";
import "../../styles/global.css";
import '../../components/styles'
import CircularProgress from "@material-ui/core/CircularProgress";

export const ButtonProgress = styled(CircularProgress)`
  position: absolute;
  top: 50%;
  left: 50%;
  margin-left: -12px;
  margin-top: -12px;
`;

export const StyledButton = styled(Button)`
  &&.MuiButtonBase-root {
    background-color: var(--color-blue-secondary-light);
    color: white;
    width: 133px;
    height: 32px;
  }

  &&.MuiButtonBase-root:hover {
    background-color: var(--color-blue-secondary-lighter);
  }
  &:focus {
    background-color: var(--color-blue-secondary-dark);
  }
  
`;