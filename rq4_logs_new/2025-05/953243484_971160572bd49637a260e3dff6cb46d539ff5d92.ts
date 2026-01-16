import { FileRoutesByFullPath } from "@/routeTree.gen";
import projectCard from "@/assets/project-card.jpg";
import visualCard from "@/assets/visual-card.jpg";
import videoCard from "@/assets/video-card.jpg";
import aboutCard from "@/assets/about-card.jpg";

export const links: {
  to: keyof FileRoutesByFullPath;
  imgSrc: string;
  label: string;
}[] = [
  { to: "/projects", imgSrc: projectCard, label: "Project" },
  { to: "/visuals", imgSrc: visualCard, label: "Visual" },
  { to: "/videos", imgSrc: videoCard, label: "Video" },
  { to: "/about", imgSrc: aboutCard, label: "About" },
];