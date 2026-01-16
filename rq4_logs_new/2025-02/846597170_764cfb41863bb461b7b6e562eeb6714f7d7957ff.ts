import { LoginMenuItem, MenuLink } from "@/types";


export const MENU_LINKS: MenuLink[] = [
  { name: "Home", link: "/", id: 1 },
  { name: "Shop", link: "/shop", id: 2 },
  { name: "About Us", link: "/about-us", id: 3 },
  { name: "Contact Us", link: "/contact-us", id: 4 },
];

export const LOGIN_MENU_ITEMS: LoginMenuItem[] = [
  { text: "Create an account", href: "/signup", state: "signup" },
  { text: "Log in to your account", href: "/login", state: "login" },
];

export const MOBILE_AUTH_ITEMS: LoginMenuItem[] = [
  {
    text: "Login",
    href: "/login",
    className: "border border-green text-green hover:bg-green/80 hover:text-white",
  },
  {
    text: "Sign Up",
    href: "/signup",
    className: "bg-green text-white hover:bg-green/80",
  },
];