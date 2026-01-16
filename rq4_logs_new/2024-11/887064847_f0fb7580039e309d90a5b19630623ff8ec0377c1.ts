import AnasArdiansyah from '../../public/assets/images/Testimonials/anas-ardiansyah.jpg'
import AsharulMaali from '../../public/assets/images/Testimonials/asharul-maali.jpg'
import FebrianAndy from '../../public/assets/images/Testimonials/febrian-andy.jpg'
import RaihanGhassani from '../../public/assets/images/Testimonials/raihan-ghassani.jpg'
import RagastraHaryono from '../../public/assets/images/Testimonials/ragastra-haryono.png'
import { StaticImageData } from 'next/image'

export type TestimonialProps = {
  name: string
  job: string
  statement: string
  image: StaticImageData
}

export const Testimonials: TestimonialProps[] = [
  {
    name: 'Anas Ardiansyah',
    job: 'Software Engineer Google',
    statement:
      'I have been using Spend.In for 6 months to manage my business expenses, and the result is very satisfying! My business finance is now more neat than before, thanks to Spend.In!',
    image: AnasArdiansyah
  },
  {
    name: 'Asharul Maali',
    job: 'Staff UMS',
    statement:
      "It's just 1 month since Im using Spend.In to manage my business expenses, but the result is very satisfying! My business finance now more neat than before, thanks to Spend.In!",
    image: AsharulMaali
  },
  {
    name: 'Febrian Andy',
    job: 'Backend Developer Indaco',
    statement:
      'Never thought that with Spend.In managing my business expenses is so easy! Been using this platform for 3 months and still counting!',
    image: FebrianAndy
  },
  {
    name: 'Raihan Ghassani',
    job: 'Data Analyst Teleperformance',
    statement:
      "\"The best\"! That's what I want to say to this platform, didn't know that there's a platform to help you manage your business expenses like this! Very recommended to you who have a big business!",
    image: RaihanGhassani
  },
  {
    name: 'Ragastra Haryono',
    job: 'Software Engineer',
    statement:
      'Never thought that with Spend.In managing my business expenses is so easy! Been using this platform for 3 months and still counting!',
    image: RagastraHaryono
  }
]