import { useState } from "react";
import styled from "styled-components";
import { FaRegHeart, FaExpandAlt, FaRegLightbulb } from "react-icons/fa";

export default function App() {
  const [toggled, setToggled] = useState(false);

  const handleToggle = () => {
    setToggled(!toggled);
  };
  return (
    <Page>
      <ToggleAttribute
        onClick={handleToggle}
        style={{ color: toggled ? "#fff" : "#222" }}
      />
      {toggled && (
        <AttributeDetails>
          <h3>Attribution</h3>
          <p>
            This project was hand coded by me{" "}
            <FaRegHeart style={{ color: "redorange" }} /> along with the
            selection of tools on how to implement the design. However, this
            design is not my own and is only intended to be used for practice. Design is credited to<br />
            <a
              href="https://www.uidesigndaily.com/posts/figma-website-section-articles-card-day-1425"
              target="_blank"
              rel="noopener noreferer"
            >
              UI Design Daily
            </a>
          </p>
        </AttributeDetails>
      )}
      <Header>
        <span>Rain</span>
        <h1>Inspirational stories</h1>
        <p>
          Find the inspiration you seek by photographers who finally embrased
          the wierdness of street photography.
        </p>
      </Header>
      <MainContent>
        <ExpandIcon />
        <h2>
          How to find the confidence to get out and shoot photography on the
          streets
        </h2>
      </MainContent>
      <AsideContent>
        <ExpandIcon />
        <header />
        <div>
          <h3>Street photography and an umbrella</h3>
          <p>
            Every creative is unique. Technique varies between individuals, and
            when it comes to the daunting task of street photography in the
            rain...
          </p>
          <ReadMore>Read story</ReadMore>
        </div>
      </AsideContent>
    </Page>
  );
}

const Page = styled.section`
  background: ${(props) => props.theme.colors.white};
  min-height: 100vh;
  width: 100%;
  position: relative;
  display: flex;
  justify-content: center;
  flex-wrap: wrap;
  overflow-x: hidden;
`;

const Header = styled.section`
  min-height: 20vh;
  width: 100%;
  display: flex;
  flex-direction: column;
  align-items: center;
  margin-top: 4rem;
  text-align: center;

  span {
    display: inline-block;
    background: ${(props) => props.theme.colors.red};
    color: ${(props) => props.theme.colors.white};
    border-radius: 20px;
    padding: 0.15rem 0.2rem;
    min-width: 50px;
  }

  h1 {
    font-weight: bolder;
    font-size: 1.5rem;
    color: ${(props) => props.theme.colors.black};
    margin: 1rem 0;
  }

  p {
    color: ${(props) => props.theme.colors.red};
    font-size: 1.25rem;
    max-width: 860px;
    margin: 0 auto;
    padding: 1rem 1.5rem;
  }
`;

const ExpandIcon = styled(FaExpandAlt)`
  position: absolute;
  top: 2%;
  right: 2%;
  font-size: 0.95rem;
  color: ${(props) => props.theme.colors.white};
  opacity: 0;
`;

const MainContent = styled.main`
  position: relative;
  max-width: 500px;
  width: 100%;
  height: 400px;
  margin: 1rem;
  border-radius: 9px;
  background-image: url("https://images.unsplash.com/photo-1515539113938-a0d7843c15fd?ixid=MnwxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8&ixlib=rb-1.2.1&auto=format&fit=crop&w=1050&q=80");
  background-size: cover;
  background-position: center;
  background-repeat: no-repeat;

  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: flex-end;

  cursor: pointer;
  transition: 0.25s all ease-in-out;

  &:hover {
    transform: translateY(-3px);
  }
  &:hover ${ExpandIcon} {
    transition: 0.25s ease-in-out;
    transition-delay: 0.5s;
    opacity: 1;
  }

  h2 {
    font-weight: bold;
    font-size: 1.75rem;
    color: ${(props) => props.theme.colors.white};
    padding: 0 1rem 1rem 1rem;
    background: rgba(0, 0, 0, 0.3);
  }
`;

const AsideContent = styled.aside`
  position: relative;
  /* Mobile First */
  max-width: 500px;
  width: 100%;
  height: 400px;
  margin: 1rem;

  /* If screen size meets AT LEAST 960px */

  @media screen and (min-width: 960px) {
    max-width: 300px;
    height: 400px;
    width: 100%;
    margin: 1rem;
  }

  cursor: pointer;
  transition: 0.25s all ease-in-out;

  &:hover {
    transform: translateY(-3px);
  }

  &:hover ${ExpandIcon} {
    transition: 0.25s ease-in-out;
    transition-delay: 0.5s;
    opacity: 1;
  }

  header {
    height: 40%;
    background-image: url("https://images.unsplash.com/photo-1519692933481-e162a57d6721?ixid=MnwxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8&ixlib=rb-1.2.1&auto=format&fit=crop&w=1050&q=80");
    background-size: cover;
    background-position: center;
    background-repeat: no-repeat;
    border-radius: 9px;
  }

  h3 {
    font-weight: bold;
    font-size: 1.25rem;
    margin: 1rem 0;
  }
`;

const ReadMore = styled.button`
  min-height: 30px;
  min-width: 100%;
  @media screen and (min-width: 960px) {
    min-width: 100px;
  }
  padding: 0.5rem;
  outline: none;
  border: 2px solid ${(props) => props.theme.colors.black};
  background: transparent;
  transition: 0.15s all ease-in-out;
  cursor: pointer;

  &:hover {
    background: ${(props) => props.theme.colors.black};
    color: ${(props) => props.theme.colors.white};
  }
`;

const ToggleAttribute = styled(FaRegLightbulb)`
  position: absolute;
  z-index: 99;
  top: 1%;
  right: 1%;
  font-size: 1.25rem;
  padding: 0.5rem;
  cursor: pointer;
`;

const AttributeDetails = styled.div`
  position: fixed;
  z-index: 9;
  height: 100vh;
  width: 100vw;
  background: rgba(0, 0, 0, 0.5);
  backdrop-filter: blur(7px);

  display: flex;
  justify-content: center;
  align-items: center;
  flex-direction: column;

  color: ${(props) => props.theme.colors.white};
  font-size: 1.25rem;

  p {
    max-width: 700px;
    padding: 1rem;
    text-align: center;

    a {
      display: inline-block;
      color: ${(props) => props.theme.colors.white};
    }
  }
`;