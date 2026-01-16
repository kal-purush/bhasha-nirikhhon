export interface FooterInfoItem {
  id: number;
  texto: string;
}

export interface FooterLinksItem {
  id: number;
  texto: string;
  link: string;
}

export interface FooterIconsItem {
  id: number;
  direccion: string;
}

export const FooterItems: FooterInfoItem[] = [
  {
    id: 1,
    texto: "Direccion: Calle falsa 123",
  },
  {
    id: 2,
    texto: "Cuidad, Pais",
  },
  {
    id: 3,
    texto: "Telefono: +54 (0) 000 000 000",
  },
  {
    id: 4,
    texto: "Correo: gymflow@gmail.com",
  },
];

export const FooterLinks: FooterLinksItem[] = [
  {
    id: 1,
    texto: "Inicio",
    link: "/home",
  },
  {
    id: 2,
    texto: "Planes",
    link: "/plans",
  },
  {
    id: 3,
    texto: "Sobre Nosotros",
    link: "/about-us",
  },
];

export const FooterIcons: FooterIconsItem[] = [
  {
    id: 1,
    direccion: "/images/facebook.svg",
  },
  {
    id: 2,
    direccion: "/images/instagram.svg",
  },
  {
    id: 3,
    direccion: "/images/linkedin.svg",
  },
];