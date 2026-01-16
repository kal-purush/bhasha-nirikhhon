import React from 'react';
import styled from 'styled-components';

const Card = styled.div`
  background: #A9FDAC;
  margin: 18px;
  padding: 12px;  
  font-weight: bold;
`
const Number = styled.p`
  font-size: 56px;
`

function ChildComp({icon, label, number}) {
  return (
    <Card>
      <p><span> {icon} </span> {label} </p>
      <Number> {number} </Number>
    </Card>
  )
}

const CreateComp = () => {
  return (
    <div className="row">
      <h1>The Challenge: Create Components in React</h1>
      <ChildComp icon='💂🏽‍♀️' label="guardian" number={150} />
      <ChildComp icon='🔮' label="magician" number={120} />
      <ChildComp icon='💻' label="developer" number={300} />
      <ChildComp icon='🩺' label="doctor" number={300} />
    </div>
  );
}

export default CreateComp;