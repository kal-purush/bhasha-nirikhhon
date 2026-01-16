import { doc, getDoc } from "firebase/firestore";
import { db } from "../../../lib/firebase";

export const getCompanyById = async (
  companyId: string
): Promise<CompanyFormTypes> => {
  try {
    const companyDocRef = doc(db, "companies", companyId);
    const companySnapshot = await getDoc(companyDocRef);
    if (!companySnapshot.exists()) {
      throw new Error("Company not found");
    }
    const companyData = companySnapshot.data() as CompanyFormTypes;
    return companyData;
  } catch (error) {
    throw error;
  }
};