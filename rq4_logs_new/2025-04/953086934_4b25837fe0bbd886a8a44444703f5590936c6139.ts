import { createContext, useContext } from "react";
import { firestoreQueries } from "@/firebase/queries";
import { orderBy } from "firebase/firestore";
import type { Material } from "@/firebase/types";

interface EstimateCalculatorContextValue {
  queryOptions: ReturnType<typeof firestoreQueries.getMaterialList>;

  materialModal: { open: boolean; material: Material | null };
  setMaterialModal: (open: boolean, material?: Material | null) => void;
}

const EstimateCalculatorContext = createContext<EstimateCalculatorContextValue>(
  {
    queryOptions: firestoreQueries.getMaterialList(orderBy("value", "desc")),

    materialModal: { open: false, material: null },
    setMaterialModal: () => null,
  }
);

const useEstimateCalculator = () => {
  const context = useContext(EstimateCalculatorContext);
  if (!context)
    throw new Error(
      "useEstimateCalculator must be used within an EstimateCalculatorProvider"
    );
  return context;
};

export { EstimateCalculatorContext, useEstimateCalculator };