import { useRef } from "react";
import { CreateAnalysisInput, createAnalysis } from "./utils/createAnalysis";


export const useCreateAnalysisAfterBiopsyUpload =  ({apiClient, laboratoryId, patientId, biopsyId}: CreateAnalysisInput) => {
  const nbOfUploadedBiopsyFilesRef = useRef<number>(0);

    const handleFileUploadCompletion = async () => {
        nbOfUploadedBiopsyFilesRef.current += 1
        const isBiopsyFullyUploaded = nbOfUploadedBiopsyFilesRef.current === 4;
        if(isBiopsyFullyUploaded){
            const analysis = await createAnalysis({apiClient, laboratoryId,patientId,biopsyId})
            console.log(analysis)
        } 
    }

    const reinitializeNbOfUploadedBiopsyFiles = ()=>{
        nbOfUploadedBiopsyFilesRef.current =0
    }

    return {
      handleFileUploadCompletion,
      reinitializeNbOfUploadedBiopsyFiles
    };
};