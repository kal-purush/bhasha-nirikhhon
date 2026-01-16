import { Variants } from 'framer-motion'

export const opacity: Variants = {
  initial: {
    opacity: 0
  },
  exit: {
    opacity: 1
  }
}

export const listVariants: Variants = {
  hidden: {
    y: 100,
    opacity: 0,
    transition: {
      when: 'afterChildren'
    }
  },
  visible: {
    y: 0,
    opacity: 1,
    transition: {
      duration: 1.25,
      delayChildren: 0.75,
      staggerChildren: 0.1
    }
  }
}

export const itemVariants: Variants = {
  hidden: {
    y: -50,
    opacity: 0
  },
  visible: {
    y: 0,
    opacity: 1,
    transition: {
      duration: 0.5
    }
  }
}