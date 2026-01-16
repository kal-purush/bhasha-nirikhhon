import image1 from "../src/assets/img/jam/01.jpg";
import image2 from "../src/assets/img/jam/02.jpg";
import image3 from "../src/assets/img/jam/03.jpg";
import image4 from "../src/assets/img/jam/04.jpg";

import AdventWindow from "./components/AdventWindow/AdventWindow";
const JamData: AdventWindow[] = [
  {
    day: "1",
    title: "Strawberry",
    imageUrl: image1,
    rating: 10,
  },
  {
    day: "2",
    title: "Grapefruit & Dragonfruit",
    imageUrl: image2,
    rating: 8,
  },
  {
    day: "3",
    title: "Mirabelle Plum & Linden Jam",
    imageUrl: image3,
    rating: 6.5,
  },
  {
    day: "4",
    title: "Apricot & Bergamot",
    imageUrl: image4,
    rating: 2,
  },
];

export default JamData;