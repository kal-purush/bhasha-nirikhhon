import { AnimationProps } from 'components/AnimatedWrapper/types/animation-props'

export const beforeChildrenTopAnimation: AnimationProps = {
  hidden: { y: -50, opacity: 0 },
  visible: {
    y: 0,
    opacity: 1,
    transition: {
      duration: 1.4,
      when: 'beforeChildren',
      staggerChildren: 0.2
    }
  }
}