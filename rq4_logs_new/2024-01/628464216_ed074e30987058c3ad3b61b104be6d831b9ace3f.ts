import styled from 'styled-components';

const AboutMe = styled.div`
  #About-Me-Page {
    background: ${(props) => props.theme.color.secundary};
    display: flex;
    flex-flow: row nowrap;
    padding: 3em 0;
    transition: 0.25s all ease-out;
    width: 100%;
  }

  .Avatar-Matheus {
    display: flex;
    flex-flow: column nowrap;
    justify-content: center;
    align-items: center;
    color: #5d2de2;
    width: 50%;
  }

  .Avatar-Matheus-Img {
    border: 3px solid #5d2de2;
    border-radius: 50%;
    width: 40%;
  }

  .Icons {
    display: flex;
    flex-flow: row nowrap;
    justify-content: space-evenly;
    width: 100%;
  }

  .About-Me-Container {
    display: flex;
    align-items: center;
    width: 50%;
  }

  .About-Me-Center {
    width: 85%;
  }

  .Abstract-About-Me {
    text-align: start;

    h2 {
      font-weight: 500;
    }

    p {
      font-weight: 400;
      line-height: 1.5em;
    }
  }

  .btn-contact {
    margin-top: 3em;
    padding: 2em 0;
    width: 30%;
  }

  @media screen and (max-width: 768px) {
    #About-Me-Page {
      align-items: center;
      display: flex;
      flex-flow: column nowrap;
      justify-content: center;
    }

    .Avatar-Matheus {
      padding: 0 0 2em 0;
      width: 100%;
    }

    .Avatar-Matheus-Img {
      width: 10em;
    }

    .Icons {
      display: none;
    }

    .About-Me-Container {
      width: 85%;
    }

    .About-Me-Center {
      width: 100%;
    }

    .Buttons-About-Me {
      .btn-contact {
        width: 100%;
      }
    }
  }
`;

export default AboutMe;