import { FaWhatsapp } from "react-icons/fa";
import { FaInstagram } from "react-icons/fa6";
import { AiOutlineYoutube } from "react-icons/ai";
import { FaLinkedin } from "react-icons/fa";
import { MenuFooterProp, SocialButtonProp } from "@/types";

export const MenuFooterData: MenuFooterProp[] = [
  {
    id: 1,
    titleMenu: "Enlaces Rápidos",
    childrens: [
      {
        id: 1,
        titleOption: "Inicio",
        linkOption: "/",
      },
      {
        id: 2,
        titleOption: "Quienes Somos",
        linkOption: "/quienes-somos",
      },
      {
        id: 3,
        titleOption: "Servicios",
        linkOption: "/servicios",
      },
      {
        id: 4,
        titleOption: "Blog",
        linkOption: "/blog",
      },
      {
        id: 5,
        titleOption: "Contacto",
        linkOption: "/contacto",
      },
    ],
  },
  {
    id: 2,
    titleMenu: "Blog",
    childrens: [
      {
        id: 1,
        titleOption: "Marketing",
        linkOption: "/blog/marketing",
      },
      {
        id: 2,
        titleOption: "Transformación Digital",
        linkOption: "/blog/transformacion-digital",
      },
      {
        id: 3,
        titleOption: "Industrias",
        linkOption: "/blog/industrias",
      },
      {
        id: 4,
        titleOption: "Negocios",
        linkOption: "/blog/negocios",
      },
      {
        id: 5,
        titleOption: "CX",
        linkOption: "/customer-experience",
      },
    ],
  },
];

export const SocialButtonData: SocialButtonProp[] = [
  {
    id: 1,
    href: "https://www.facebook.com/MarketerosAlMundo",
    iconName: FaWhatsapp,
    target: "_blank",
    srOnly: "Whatsapp",
  },
  {
    id: 2,
    href: "https://www.instagram.com/marketing_al_dia/",
    iconName: FaInstagram,
    target: "_blank",
    srOnly: "Instagram",
  },
  {
    id: 3,
    href: "https://www.linkedin.com/company/65540696/admin/page-posts/published/",
    iconName: FaLinkedin,
    target: "_blank",
    srOnly: "Linkedin",
  },
  {
    id: 4,
    href: "https://www.youtube.com/@marketingaldiaperu",
    iconName: AiOutlineYoutube,
    target: "_blank",
    srOnly: "Youtube",
  },
];