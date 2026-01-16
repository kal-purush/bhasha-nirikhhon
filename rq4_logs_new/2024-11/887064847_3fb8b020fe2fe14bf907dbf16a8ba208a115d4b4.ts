import { Expand, Rocket, Star, Users } from 'lucide-react'

export type WhyUsProps = {
  id: number
  title: string
  description: string
  icon: React.ElementType
}

export const WhyUs: WhyUsProps[] = [
  {
    id: 1,
    title: 'A Team You Can Trust',
    description:
      'Our team of skilled developers and IT consultants combines technical expertise with a commitment to excellence. We build long-lasting relationships through transparency, reliability, and top-notch service.',
    icon: Users
  },
  {
    id: 2,
    title: 'Innovative Solutions',
    description:
      'We specialize in creating custom software and IT solutions that align perfectly with your business goals. Our innovative approach ensures that your technology evolves with your needs.',
    icon: Rocket
  },
  {
    id: 3,
    title: 'Scalable',
    description:
      'Our solutions are built to grow with your business. Whether you’re expanding your team, launching new products, or entering new markets, our systems are designed to handle your evolving needs seamlessly.',
    icon: Expand
  },
  {
    id: 4,
    title: 'Personalized',
    description:
      'We understand that every business is unique. That’s why we take the time to listen, analyze, and deliver customized strategies and technologies that align perfectly with your specific goals.',
    icon: Star
  }
]