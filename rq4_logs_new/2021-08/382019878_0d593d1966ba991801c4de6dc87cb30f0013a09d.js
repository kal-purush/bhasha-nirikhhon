import React from "react";
import PropTypes from "prop-types";
import styled from "styled-components";

const MainTemplate = ({ children }) => {
  return <Main>{children}</Main>;
};

MainTemplate.propTypes = {};

export default MainTemplate;
export const Main = styled.main`
  padding-top: 60px;
  max-width: 1280px;
  width: 90%;
  margin: 0 auto;
`;