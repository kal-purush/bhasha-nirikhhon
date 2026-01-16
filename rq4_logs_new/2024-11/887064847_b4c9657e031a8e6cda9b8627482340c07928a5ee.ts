import { SiInstagram, SiLinkedin, SiX } from '@icons-pack/react-simple-icons'

export type FooterLinkProps = {
  label: string
  href: string
}

export type ListFooterLinkProps = {
  Home: FooterLinkProps[]
  Help: FooterLinkProps[]
  Company: FooterLinkProps[]
}

export const ListFooterLink: ListFooterLinkProps = {
  Home: [
    { label: 'Blog', href: '/blog' },
    { label: 'Services', href: '/services' },
    { label: 'About Us', href: '/about' },
    { label: 'Contact Us', href: '/contact' }
  ],
  Help: [
    { label: 'FAQ', href: '/faq' },
    { label: 'Accessibility', href: '/accessibility' },
    { label: 'Privacy Policy', href: '/privacy-policy' }
  ],
  Company: [
    { label: 'Career', href: '/career' },
    { label: 'Pricing', href: '/pricing' }
  ]
}

export type FooterSectionProps = {
  title: keyof ListFooterLinkProps
  links: FooterLinkProps[]
}

type SocialLinkProps = {
  href: string
  icon: React.ElementType
  label: string
}

export const socialLinks: SocialLinkProps[] = [
  { href: 'https://www.instagram.com/domsat.id/', icon: SiInstagram, label: 'Instagram' },
  { href: 'https://linkedin.com', icon: SiLinkedin, label: 'LinkedIn' },
  { href: 'https://twitter.com', icon: SiX, label: 'X' }
]