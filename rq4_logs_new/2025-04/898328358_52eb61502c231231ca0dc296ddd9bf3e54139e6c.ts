// /lib/firebase/getDesignationFirestoreData.ts
import { db } from "@/firebase";
import { collection, getDocs, query, where } from "firebase/firestore";
import { School } from "@/app/types/school";

type Params = {
  course?: string;
  totalTuitionFee?: number;
  movingOutsideThePrefecture?: boolean;
  commutingStyle?: string;
  highSchool?: string;
  attendanceFrequency?: string;
};

export const getDesignationFirestoreData = async ({
  totalTuitionFee,
  movingOutsideThePrefecture,
  commutingStyle,
  highSchool,
  attendanceFrequency,
}: Params): Promise<School[]> => {
  try {
    const schoolRef = collection(db, "schools");
    const filters = [];

    if (typeof totalTuitionFee === "number" && !isNaN(totalTuitionFee)) {
      filters.push(where("totalTuitionFee", "<=", totalTuitionFee));
    }

    if (typeof movingOutsideThePrefecture === "boolean") {
      filters.push(
        where("movingOutsideThePrefecture", "==", movingOutsideThePrefecture)
      );
    }

    if (commutingStyle) {
      filters.push(where("commutingStyle", "==", commutingStyle));
    }

    if (highSchool) {
      filters.push(where("highSchool", "==", highSchool));
    }

    if (attendanceFrequency) {
      filters.push(where("attendanceFrequency", "array-contains", attendanceFrequency));
    }

    const q = query(schoolRef, ...filters);
    const snapshot = await getDocs(q);

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
  } catch (error) {
    console.error("指定条件での取得に失敗しました:", error);
    return [];
  }
};