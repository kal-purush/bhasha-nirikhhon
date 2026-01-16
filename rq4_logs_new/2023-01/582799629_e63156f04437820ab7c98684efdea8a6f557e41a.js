import React from 'react'
import { Link } from 'react-router-dom'
import Card from 'react-bootstrap/Card';


const Item = ({producto}) => {

  return (
    <article key={producto.id}>
        <Card style={{ width: '18rem' }}>
          <Card.Img variant="top" src={producto.image} alt={producto.title}/>
          <Card.Body>
            <Card.Title>{producto.title}</Card.Title>
            <Card.Text>
              Price: {producto.price}
            </Card.Text>
            <Link to={"/item/" + producto.id}>Ver mas informacion</Link>
          </Card.Body>
        </Card>
    </article>
  )
}

export default Item