// footerData.ts
import { FaLinkedin, FaWhatsapp } from 'react-icons/fa';
import { FooterSection } from '@/types';

export const footerData: FooterSection[] = [
  {
    title: 'Company Information',
    links: [
      {
        name: 'Email Us',
        href: 'mailto:info@company.com',
      },
      {
        name: 'Call Us',
        href: 'tel:+1234567890',
      },
      {
        name: 'Our Location',
        href: '/location', // Update this link if you have a location page
      },
    ],
  },
  {
    title: 'Support',
    links: [
      {
        name: 'LinkedIn',
        href: 'https://www.linkedin.com/company/your-company',
        icon: FaLinkedin, // Assign the icon as a component
      },
      {
        name: 'WhatsApp',
        href: 'https://wa.me/yourwhatsappnumber',
        icon: FaWhatsapp, // Assign the icon as a component
      },
      {
        name: 'Contact Form',
        href: '/contact',
      },
      {
        name: 'Schedule a Call',
        href: '/contact',
      },
    ],
  },
  {
    title: 'Quick Links',
    links: [
      {
        name: 'Company Overview',
        href: '/about',
      },
      {
        name: 'Our Team',
        href: '/team',
      },
      {
        name: 'Careers',
        href: '/careers',
      },
      {
        name: 'Blog',
        href: '/blog',
      },
      {
        name: 'Privacy Policy',
        href: '/privacy-policy',
      },
      {
        name: 'Terms of Service',
        href: '/terms',
      },
    ],
  },
];