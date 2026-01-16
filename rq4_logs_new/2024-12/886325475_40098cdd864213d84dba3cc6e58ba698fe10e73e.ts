import { getFirestore, DocumentData , doc, getDoc } from "firebase/firestore";
import {app} from './firebaseConfig';
import { ITimelineData } from "@/interfaces/ITimeline";
import { ISponsor } from "@/interfaces/ISponsors";
import { ICompany } from "@/interfaces/ICompanies";


const AGGREGATED_DOC_PATH = "metadata/agregatedCompanies";

const getAggregatedCompanyDoc = async ():Promise<DocumentData> => {
    const db = getFirestore(app);
    const docRef = doc(db, AGGREGATED_DOC_PATH);
    const docSnap = await getDoc(docRef);
    return docSnap
}

export const getDataFromAggregatedCompanyDoc = async ():Promise<{companies:ICompany[]}> => {
    const aggregatedDoc = await getAggregatedCompanyDoc();
    console.log("Aggregated Company Doc",aggregatedDoc);
    console.log("Aggregated Company Doc Data",aggregatedDoc.data());
    const data = aggregatedDoc.data();

    if(!data){
        return {companies:[]};
    }

    let companies: ICompany[] = [];

    try {
        if (data["companies"]) {
            companies = Object.keys(data["companies"])
            .map(key => ({ id: key, ...data["companies"][key] })) as ICompany[];
            }
    } catch (error) {
        console.error("Error processing company data:", error);
    }

    return {companies};
}