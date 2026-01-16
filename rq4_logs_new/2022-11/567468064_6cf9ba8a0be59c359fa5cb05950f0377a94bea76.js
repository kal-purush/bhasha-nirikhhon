import React, { useEffect, useState } from "react";
import Row from "react-bootstrap/Row";
import Container from "react-bootstrap/Container";
import Carousel from "react-bootstrap/Carousel";
import Card from "react-bootstrap/Card";

import { work } from "../helpers/portfolio";
import Stack from "react-bootstrap/Stack";

const Portfolio = () => {
  const [width, setWidth] = useState(window.innerWidth);

  function handleWindowSizeChange() {
    setWidth(window.innerWidth);
  }
  useEffect(() => {
    window.addEventListener("resize", handleWindowSizeChange);
    return () => {
      window.removeEventListener("resize", handleWindowSizeChange);
    };
  }, []);

  const isDesktop = width >= 768;
  return (
    <section className="bg-dark text-white p-4">
      <Container>
        <h2 className="my-4 text-center f-large">Styled by Laura</h2>
        <Carousel>
          {work.map((projObj, idx) => {
            return (
              <Carousel.Item key={idx}>
                <Stack
                  direction={isDesktop ? "horizontal" : "vertical"}
                  className="justify-content-center align-items-center pro-stack"
                  gap={3}
                >
                  {projObj.slideData.map((imgUrl, idx) => {
                    return (
                      <Card key={idx}>
                        <Card.Img
                          src={imgUrl}
                          alt="frfd"
                          fluid
                          className={isDesktop ? "" : "mobile-card-img"}
                        />
                      </Card>
                    );
                  })}
                </Stack>
              </Carousel.Item>
            );
          })}
        </Carousel>
      </Container>
    </section>
  );
};

export default Portfolio;