import { collection, getDocs } from "firebase/firestore";
import { db } from "@/firebase";
import { School } from "@/app/types/school";

export const getFirestoreData = async (): Promise<School[]> => {
  const snapshot = await getDocs(collection(db, "schools"));
  return snapshot.docs.map((doc) => {
    const data = doc.data();
    return {
      id: doc.id,
      name: data.name,
      course: data.course,
      totalTuitionFee: data.totalTuitionFee,
      firstYearFee: data.firstYearFee,
      secondYearFee: data.secondYearFee,
      thirdYearFee: data.thirdYearFee,
      testFee: data.testFee,
      movingOutsideThePrefecture: data.movingOutsideThePrefecture,
      commutingStyle: data.commutingStyle,
      highSchool: data.highSchool,
      url: data.url,
      imgUrl: data.imgUrl,
      attendanceFrequency: data.attendanceFrequency,
    };
  });
};