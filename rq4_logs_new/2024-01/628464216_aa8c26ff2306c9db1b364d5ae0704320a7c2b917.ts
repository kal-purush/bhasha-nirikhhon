import styled from 'styled-components';

const HomePage = styled.div`
  @import url('https://fonts.googleapis.com/css2?family=Merienda:wght@400;700&display=swap');

  #Home-Page {
    height: 100vh;
    padding-top: 8%;
    transition: 0.25s all ease-out;
    width: 100%;
  }

  #luxbar {
    background: ${(props) => props.theme.color.secundary};
    -webkit-box-shadow: 0px -6px 10px 8px rgba(0, 0, 0, 0.43);
    box-shadow: 0px -6px 10px 8px rgba(0, 0, 0, 0.15);
    position: fixed;
    top: 0;
    transition: 0.25s all ease-out;
    width: 100%;
    z-index: 100;
  }

  .luxbar-menu {
    transition: 0.25s all ease-out !important;
  }

  .luxbar-menu-dark {
    background: ${(props) => props.theme.color.secundary};
    color: ${(props) => props.theme.color.text};
    padding: 0 2rem;
  }

  .luxbar-hamburger span::before,
  .luxbar-hamburger span::after {
    background: ${(props) => props.theme.color.text};
  }

  .luxbar-hamburger span {
    background: ${(props) => props.theme.color.text};
  }

  .luxbar-item {
    align-items: center;
    display: flex;
  }

  .luxbar-item:hover {
    background: none;
    color: #5d2de2;
    transition: 0.25s all ease-out;
  }

  .Title-Name {
    font-family: 'Merienda', cursive;
    font-size: 1.2em;
  }

  .Light-Theme-Icon {
    margin-left: 0.3em;
    margin-top: 0.1em;
  }

  /* CONTEÚDO PRINCIPAL */
  .Main-Content {
    display: flex;
    justify-content: space-around;
    height: 75%;
    width: 100%;
  }

  .Main-Content .Contacte-Me-Container {
    align-items: center;
    display: flex;
    justify-content: center;
    flex: 53;
  }

  .summary-about-me {
    font-size: 1.5em;
  }

  .Student-Web {
    font-weight: 500;
  }

  .buttons-contact {
    padding-top: 0.3rem;

    .btn-github {
      margin-left: 0.4em;
    }
  }

  .Main-Content .illustration-dev-container {
    align-items: center;
    display: flex;
    justify-content: center;
    flex: 47;
  }

  .buttons-contact {
    padding-top: 1rem;

    .btn-github {
      margin-left: 0.4em;
    }
  }

  .illustration-dev {
    width: 70%;
  }

  .Scrool-Text {
    margin-bottom: 0.5em;
    margin-top: -1em;
  }

  .arrow {
    position: absolute;
    top: 90%;
    left: 50%;
    transform: translate(-50%, -50%);
  }

  .arrow span {
    display: block;
    width: 15px;
    height: 15px;
    border-bottom: 3px solid #5d2de2;
    border-right: 3px solid #5d2de2;
    transform: rotate(45deg);
    animation: animate 2s infinite;
  }

  .arrow span:nth-child(2) {
    animation-delay: -0.2s;
  }

  .arrow span:nth-child(3) {
    animation-delay: -0.4s;
  }

  @keyframes animate {
    0% {
      opacity: 0;
      transform: rotate(45deg) translate(-20px, -20px);
    }
    50% {
      opacity: 1;
    }
    100% {
      opacity: 0;
      transform: rotate(45deg) translate(20px, 20px);
    }
  }

  .Mouse-Seta {
    color: #5d2de2;
  }

  @media screen and (max-width: 768px) {
    #Home-Page {
      padding-top: 8%;
    }

    .Header {
      display: none;
    }

    .luxbar-navigation .luxbar-item {
      margin: 0 auto;
    }

    .Main-Content {
      align-items: center;
      display: flex;
      flex-flow: column nowrap;
      height: 78%;
      justify-content: space-evenly;
    }

    .Main-Content .Contacte-Me-Container {
      height: 30%;
      text-align: center;
      width: 80%;

      .summary-container {
        margin-top: 5em;
      }
    }

    .buttons-contact {
      display: flex;
      flex-flow: column nowrap;

      .btn-contact {
        width: 100%;
      }

      .btn-github {
        margin-left: 0;
      }
    }

    .btn-contact {
      margin-top: 1em;
      width: 100%;
    }

    .Main-Content .illustration-dev-container {
      display: none;
    }

    .Btn-Contact {
      height: 3em;
    }
  }
`;

export default HomePage;