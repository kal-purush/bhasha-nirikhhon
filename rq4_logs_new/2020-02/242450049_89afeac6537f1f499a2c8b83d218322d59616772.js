import React from 'react';
import { Card, Button, Carousel } from 'react-bootstrap';

export default function LandingPage(){

    return(
        <div className = "Landing Page">
            <Carousel>
              <Carousel.Item>
                <img
                  className="d-block w-100 h-50"
                  src="https://www.jacobimages.com/home/wp-content/uploads/2011/08/Isabela_1147_5981.jpg"
                  fluid
                />
                <Carousel.Caption>
                  <h1>Social Network</h1>
                  <p>Nulla vitae elit libero, a pharetra augue mollis interdum.</p>
                </Carousel.Caption>
              </Carousel.Item>
              <Carousel.Item>
                <img
                  className="d-block w-100"
                  src="https://www.savethechildren.org.ph/__resources/userfiles/image/program-stories/watch_language_is_our_only_difference/02.jpg"
                  fluid
                />

                <Carousel.Caption>
                  <h3>Second slide label</h3>
                  <p>Lorem ipsum dolor sit amet, consectetur adipiscing elit.</p>
                </Carousel.Caption>
              </Carousel.Item>
              <Carousel.Item>
                <img
                  className="d-block w-100"
                  src="https://www.jacobimages.com/home/wp-content/uploads/2011/08/Isabela_7695_14891.jpg"
                  fluid
                />

                <Carousel.Caption>
                  <h3>Third slide label</h3>
                  <p>Praesent commodo cursus magna, vel scelerisque nisl consectetur.</p>
                </Carousel.Caption>
              </Carousel.Item>
            </Carousel>
        </div>
    );
    
}