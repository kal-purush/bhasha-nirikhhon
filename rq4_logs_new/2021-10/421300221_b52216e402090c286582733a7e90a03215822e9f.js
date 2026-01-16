import React from "react";
import styled from "styled-components";
import Navbar from "../../components/Navbar";
import Footer from "../../components/Footer";
import RegisterElement1 from "./RegisterElement1";
const Register = () => {
  return (
    <>
      <RegisterWrapper>
        <Navbar />
        <RegisterElement1 />
        <Footer />
      </RegisterWrapper>
    </>
  );
};

export default Register;

const RegisterWrapper = styled.div`
  width: 100%;
`;