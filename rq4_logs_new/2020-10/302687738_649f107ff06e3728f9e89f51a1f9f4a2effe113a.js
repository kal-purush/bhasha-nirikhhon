import React from "react";
import { Div, Icon, Container, Text } from "atomize";

const Loading = () => {
  return (
    <Container>
      <Div d="flex" flexDir='column' align="center" justify="center" p='5rem' h='75vh'>
        <Icon name="Alert" size="80px" />
        <Text tag='h2' textSize='display3'>Something has gone wrong!</Text>
        <Text tag='p' textSize='display1'>Please check back later</Text>
      </Div>
    </Container>
  );
};

export default Loading;