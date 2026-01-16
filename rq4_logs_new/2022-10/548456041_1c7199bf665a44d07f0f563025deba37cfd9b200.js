import { useState, useEffect } from "react";
import { Card, CardBody } from "reactstrap";
import { useSpring, animated } from "react-spring";

export const AnimatedAnswerCard = ( {text} ) => {
  const [toggle, setToggle] = useState(false);

  const animatedStyle = useSpring({
    opacity: toggle ? 1 : 0,
    transform: toggle ? "scale(1,1)" : "scale(1,0)",
    config: { duration: 1000 }
  });

  useEffect(() => {
    setToggle(true);
  },[]);

  return (
    <>
      <animated.div style={animatedStyle}>
        <Card className="card card-grow">
          <CardBody className="card-body text-center">
            <p className="card-text">{text}</p>
          </CardBody>
        </Card>
      </animated.div>
    </>
  );
};

export default AnimatedAnswerCard;