import React from 'react';
import styled from 'styled-components';
import PropTypes from 'prop-types';

const Container = styled.fieldset`
  display: flex;
  width: 100%;
`;

const Title = styled.p`
  font-size: calc(1rem + 1vw);
  margin-bottom: 20px;
  font-family: ${props => props.theme.fontFamily.main}, sans-serif;
  padding: 10px;
  min-width: 100%;
  box-sizing: border-box;

  @media(max-width: 520px) {
    padding: 0;
    margin-bottom: 10px;
  }
`;

const Label = styled.label`
  font-size: calc(1.3rem + 0.5vw);
  font-family: ${props => props.theme.fontFamily.main}, sans-serif;
  display: flex;
  flex-direction: column;
  align-items: center;
  padding: 5px;
  min-width: 15rem;
  flex: 1;
  margin-right: 0;

  :before {
    content: "";
    display: block;
    width: 5em;
    height: 5em;
    background-image: url('${({backgroundImage}) => backgroundImage}');
    background-position: center;
    background-size: contain;
    background-repeat: no-repeat;
  }
`;

const Checkbox = styled.span`
  width: 1em;
  height: 1em;
  position: relative;
  background: white;
  padding: 0.2em;
  border-radius: 100%;
  transition: 150ms ease-in-out all;
  border: 2px solid ${props => props.theme.color.lightBlue};

  :before {
    content: "\u2713";
    font-weight: 700;
    width: 100%;
    height: 100%;
    position: absolute;
    color: ${props => props.theme.color.lightBlue};
    opacity: 0;
    transition: inherit;
    top: 50%;
    left: 50%;
    transform: translate(-25%, -50%);
  }

  input[type='checkbox']:checked ~ &:before {
    opacity: 1;
  }

  input:focus ~ & {
    background: ${props => props.theme.color.lightBlue};

    :before {
      color: white;
    }
  }
`;

const OtherInput = styled.input`
  font-size: calc(0.85rem + 1vw);
  font-family: ${props => props.theme.fontFamily.main}, sans-serif;
  border: none;
  border-bottom: 1px solid black;
  background: none;
  outline: none;
  width: 100%;
  margin-left: 10px;
`;

const CheckboxInput = styled.input`
  position: absolute;
  opacity: 0;
  cursor: default;
`;

const OtherLabel = Label.extend`
  margin-left: 0px;
  align-self: flex-end;

  :before {
    content: none;
  }
`;

const OptionsDiv = styled.div`
  display: flex;
  flex-flow: row wrap;
  width: 100%;
  justify-content: space-between;
`;

const MultipleImageQuestion = ({
  questionText, 
  includeOpenAnswer, 
  onChange,
  name,
  options
}) => {
  return (
    <Container>
      <Title> {questionText} </Title>
      <OptionsDiv>
        {options.map(option => (
          <Label 
            key={option.name}
            backgroundImage={option.image}
          >
            <CheckboxInput 
              type="checkbox"
              name={`${name}-${option.name}`}
              onChange={onChange}
            />
            {option.name}
            <Checkbox />
          </Label>
        ))}
        {includeOpenAnswer && (
          <OtherLabel>
            Other:
            <OtherInput 
              questionText="other"
              onChange={onChange}
              name={`${name}-other`}
            />
          </OtherLabel>
        )}
      </OptionsDiv>
    </Container>
  )
};

MultipleImageQuestion.propTypes = {
  questionText: PropTypes.string.isRequired,
  onChange: PropTypes.func.isRequired,
  name: PropTypes.string.isRequired,
  options: PropTypes.arrayOf(PropTypes.shape({
    name: PropTypes.string.isRequired,
    image: PropTypes.string.isRequired
  })).isRequired,
  includeOpenAnswer: PropTypes.bool
};

MultipleImageQuestion.defaultProps = {
  includeOpenAnswer: false,
};

export default MultipleImageQuestion;
  