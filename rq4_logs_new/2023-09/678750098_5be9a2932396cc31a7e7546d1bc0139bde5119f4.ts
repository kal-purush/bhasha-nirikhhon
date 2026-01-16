import { LinkItem } from '@/components/footer/Footer';

const linkItems: LinkItem[] = [
  { label: "About Us", href: "/" },
  { label: "Terms & Conditions", href: "/" },
  { label: "Privacy Policy", href: "/" },
  { label: "Help", href: "/" },
  { label: "Tours", href: "/" },
];

const contactItems: LinkItem[] = [
  { label: "Passeio dos Descobrimentos", href: "/" },
  { label: "8600-315 Lagos", href: "/" },
  { label: "+999 9999 999 999", href: "/" },
  { label: "boatripseu@gmail.com", href: "/" },
];


export const footerBlocks = [
  {
    'Quick Links': linkItems,
  },
  {
    'Support': linkItems,
  },
  {"Contact info": contactItems},
]