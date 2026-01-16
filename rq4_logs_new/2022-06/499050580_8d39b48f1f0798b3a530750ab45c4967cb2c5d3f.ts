// import de l'interface pour typescript
import { IAboutInfo } from '../src/interfaces/IAboutInfo';

// Tableau de tous les produits
const aboutInfo: IAboutInfo[] = [
  {
    id: 1,
    title: 'Descriptif de la marque',
    text: " Brille est une marque écoresponsable et vegan de sacs à main fabriqués en Espagne dans un atelier à taille humaine, au cœur de la région d'Ubrique.  ",
    image: '/assets/images/tissus.png',
  },
  {
    id: 2,
    title: 'Descriptif des matériaux',
    text: "Ses douces matières végétales, imitant à la perfection le cuir, sont produites dans le nord de l'Italie, à base de déchets de pommes. Son coton intérieur est une matière écoresponsable produite en Espagne. Brille propose alors un produit qualitatif avant tout, durable, éthique, bon pour la planète, chic, glamour et aux couleurs vives, pour briller en toute circonstance, où que vous soyez.",
    image: '/assets/images/laine.png',
  },
  {
    id: 3,
    title: 'Notre atelier',
    text: "Nous sommes situés en Espagne dans un atelier à taille humaine, au cœur de la région d'Ubrique ainsi que dans le nord de l'Italie pour travailler nos divers matières végétales",
    image: '/assets/images/atelier.png',
  },
  {
    id: 4,
    title: 'Engagé pour notre planète',
    text: "Marque écoresponsable et vegan, recyclage de différents matériaux permettant d'attenuer les dégats de la fast fashion",
    image: '/assets/images/nature.png',
  },
];

export default aboutInfo;