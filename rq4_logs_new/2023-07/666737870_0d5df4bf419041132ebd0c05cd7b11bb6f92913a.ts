import Proj1 from "../assets/Proj01task.png";
import Proj2 from "../assets/proj2.png";
// import Proj3 from "../assets/proj3.webp";
// import Proj4 from "../assets/proj4.webp";
// import Proj5 from "../assets/proj5.jpg";
// import Proj6 from "../assets/proj6.png";

export interface ProjectItem {
  name: string;
  image: string;
  slug: string;
  skills: string;
  url: string;
}

export const ProjectList: ProjectItem[] = [
  {
    name: "Task reminder",
    image: Proj1,
    slug: "task-reminder",
    skills: "HTML,CSS,JavaScript",
    url: "https://github.com/amitbhsingh/task-reminder",
  },
  {
    name: "Blogging website",
    image: Proj2,
    slug: "blog",
    skills: "React",
    url: "https://blgn.netlify.app/",
  },
  // {
  //   name: "Spotify Clone",
  //   image: Proj3,
  //   skills: "React,Node.js,MongoDB,SpotifyAPI",
  // },
  // {
  //   name: "Social Media Website",
  //   image: Proj4,
  //   skills: "React,Node.js,MySQL,GraphQL",
  // },
  // {
  //   name: "Dashboard Visualizer",
  //   image: Proj5,
  //   skills: "JavaScript,HTML,CSS",
  // },
  // {
  //   name: "Mobile Game",
  //   image: Proj6,
  //   skills: "React Native,JavaScript,HTML,CSS",
  // },
];