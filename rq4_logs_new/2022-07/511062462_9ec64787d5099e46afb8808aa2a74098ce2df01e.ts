import topSport from "./images/categories/sport/michael-jordan-jump-shot.webp";
import topSpace from "./images/categories/space/web-first-images.png";
import topScience from "./images/categories/science/science-lab.webp";
import topEconomy from "./images/categories/economics/bitcoin.webp";
import topPolitics from "./images/categories/politics/United-Nations.jpeg";

export const topViewedPosts = [
  {
    id: 0,
    title: "G.O.A.T",
    image: topSport,
    description: "The rise of the G.O.A.T",
    category: {
      id: 0,
      title: "Sport",
      color: "orange",
    },
  },
  {
    id: 1,
    title: "The massive blow!",
    image: topSpace,
    description: "Now we can see what is going on up there",
    category: {
      id: 1,
      title: "Space",
      color: "purple",
    },
  },
  {
    id: 2,
    title: "New gravity!",
    image: topScience,
    description: "Phisiscists at CalTech discovered new type of gravity!",
    category: {
      id: 3,
      title: "Science",
      color: "lightblue",
    },
  },
  {
    id: 6,
    title: "Thats going to be intense!",
    image: topEconomy,
    description: "War in Ukraine will be hard for everyone",
    category: {
      id: 4,
      title: "Economy",
      color: "gold",
    },
  },
  {
    id: 7,
    title: "War",
    image: topPolitics,
    description: "Russia invaded into Ukraine",
    category: {
      id: 5,
      title: "Politics",
      color: "brown",
    },
  },
];