import type { ReactNode, Dispatch, SetStateAction } from "react";
import { createContext, useContext, useState } from "react"

export interface FormContextTypes {
    currentStep: number
    setCurrentStep?: Dispatch<SetStateAction<number>>
    totalSteps: number
}

// Default values with a no-op function
// export const defaultValues: FormContextTypes = {
//     currentStep: 1,
//     setCurrentStep: ()=>{},
//     totalSteps: 3,
// }

export const FormContext = createContext<FormContextTypes|undefined>(undefined)

export const useFormContext = () => useContext(FormContext)

export const FormProvider = ({ children }: { children: ReactNode }) => {
  const [currentStep, setCurrentStep] = useState(1)

  return {
    contextValue: {  currentStep, setCurrentStep, totalSteps: 3  },
    children, 
  }
}