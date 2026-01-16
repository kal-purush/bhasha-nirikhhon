import React from 'react';
import { Container, Row } from 'react-bootstrap';
import Header from './header';
import SubHeader from './subHeader';
import Footer from './footer';

function Finish() {
  return (
    <>
      <Container fluid className='container'>
        <Header />
        <SubHeader />
        <Row>
          <span>THANK YOU!</span>
        </Row>
        <Footer />
      </Container>
    </>
  );
}

export default Finish;