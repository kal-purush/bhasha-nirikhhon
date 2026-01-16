import './App.css';
import data from "./data";
import { Container, Row, Col } from 'reactstrap';


function App() {
  return (
    <div className="container main" >
      <Container>




      <Row>
        <Col><p>
   Eating a healthy diet is not about strict limitations, staying unrealistically thin, or depriving yourself of the foods you love. Rather, it’s about feeling great, having more energy, improving your health, and boosting your mood.
   </p>
   </Col>
      </Row>
      
      <Row>
        <Col><img
      src={data[0].imgUrl}
      
    />
    </Col>
      </Row>
</Container>
  
   
  </div>
  );
}

export default App;