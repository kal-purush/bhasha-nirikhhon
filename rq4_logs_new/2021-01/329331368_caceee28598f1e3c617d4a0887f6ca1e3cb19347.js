import React from "react";
import styled from "styled-components";
import Logo from "../assets/logo.png";

const Home = () => {
  return (
    <Body>
        <LogoIcon>
            <img src={Logo} alt=""></img>
        </LogoIcon>
      <MainLink href=" ">Hello.</MainLink>
      <SubLink1 href=" ">I’m</SubLink1>
      <SubLink2 href=" ">Bhanu</SubLink2>
      <SubLink3 href=" ">Prakash</SubLink3>
    </Body>
  );
};

export default Home;

const MainLink = styled.a`
  text-decoration: none;
  position: absolute;
  width: 600px;
  height: 150px;
  left: 120px;
  top: 250px;

  font-family: Montserrat;
  font-style: normal;
  font-weight: bold;
  font-size: 150px;
  line-height: 183px;

  color: #ff7a00;
  
  
`;
const SubLink1 = styled.a`
  text-decoration: none;
  position: absolute;
  width: 600px;
  height: 150px;
  left: 120px;
  top: 400px;

  font-family: Montserrat;
  font-style: normal;
  font-weight: bold;
  font-size: 150px;
  line-height: 183px;

  color: #ffffff;
`;
const SubLink2 = styled.a`
  text-decoration: none;
  position: absolute;
  width: 600px;
  height: 150px;
  left: 120px;
  top: 550px;

  font-family: Montserrat;
  font-style: normal;
  font-weight: bold;
  font-size: 150px;
  line-height: 183px;

  color: #ffffff;
`;
const SubLink3 = styled.a`
  text-decoration: none;
  position: absolute;
  width: 600px;
  height: 150px;
  left: 120px;
  top: 700px;

  font-family: Montserrat;
  font-style: normal;
  font-weight: bold;
  font-size: 150px;
  line-height: 183px;

  color: #ffffff;
`;

const Body = styled.div`
  background-color: #000000;
  position: relative;
  width: 100vw;
  height: 100vh;
`;
/* const LinkWrapper = styled.div`
  position: absolute;
  width: 602px;
  height: 633px;
  left: 120px;
  top: 250px;
`;
 */

const LogoIcon = styled.div`
  position: absolute;
  width: 150px;
  height: 120.5px;
  padding-left: 80vw;
  top: 100px;
`;