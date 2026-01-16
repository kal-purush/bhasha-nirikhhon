import { type Step } from 'intro.js-react'

export interface WalkthroughStep extends Step {
  isLastStep?: boolean
}