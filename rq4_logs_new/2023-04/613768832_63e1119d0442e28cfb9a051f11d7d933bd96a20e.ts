import { doc, getDoc } from "firebase/firestore";
import { db } from "../../../lib/firebase";

export const getJobById = async (jobId: string): Promise<Job | null> => {
  try {
    // Get a reference to the specific job document in the Firestore
    const jobDocRef = doc(db, "jobs", jobId);

    // Get the document data
    const jobSnapshot = await getDoc(jobDocRef);

    if (jobSnapshot.exists()) {
      // If the document exists, return the job data along with its ID
      const jobData = jobSnapshot.data() as Job;
      return {
        ...jobData,
        id: jobSnapshot.id,
      };
    } else {
      // If the document does not exist, return null
      return null;
    }
  } catch (e) {
    // Throw any errors that occurred during the execution of the function
    throw e;
  }
};