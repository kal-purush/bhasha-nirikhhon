import './App.css';
import data from "./data/images.json";
import { Wrap, WrapItem } from "@chakra-ui/react";
import ImageCard from "./imageCard.js";
import { Heading } from "@chakra-ui/react";
import { Button, ButtonGroup } from "@chakra-ui/react";
import { Link } from "@chakra-ui/react";
import {useSpring, animated} from 'react-spring'

function App() {
  const imageArray = data.images;
  //directly importing code from the react spring to test out here
  //doing the math to calculate the movement i think
  const calc = (x, y) => [-(y - window.innerHeight / 2) / 20, (x - window.innerWidth / 2) / 20, 1.1]
  const trans = (x, y, s) => `perspective(600px) rotateX(${x}deg) rotateY(${y}deg) scale(${s})`
  //configuring use spring
  const [props, set] = useSpring(() => ({ xys: [0, 0, 1], config: { mass: 5, tension: 350, friction: 40 } }))
  return (
    <div className="App" id="top">
      <Heading 
        as="h2" 
        size="lg" 
        margin="50px" 
        textAlign="center" 
      >
        Top Pics from 
        <Link margin="0 10px"color="orange.300" href="https://old.reddit.com/r/nocontextpics/top/" isExternal>
          r/nocontextpics
        </Link>
      </Heading> 
      <Wrap spacing="30px" align="center" justify="center">           
        {/*for some reason the map the image isnt the key its the link to each image*/}
        {(imageArray[0] === undefined) ? console.log("undefined") : imageArray.map((image) => <ImageCard link={image}/>)} 
      </Wrap>
      <Button as="a" href="#top" margin="50px" textAlign="center" colorScheme="orange" size="md">
        Back to Top
      </Button>
      
    </div>
  );
}

export default App;
