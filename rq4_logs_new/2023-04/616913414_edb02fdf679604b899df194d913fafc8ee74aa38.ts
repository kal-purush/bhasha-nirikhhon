import { atom, selector } from "recoil";
import ImgChoiceFaculty2 from "@/assets/StudyPlan/choicefaculty2.svg";

export const optionCourseState = atom<Array<OptionSubject>>({
    key: "option-course-state",
    default: [
      {
        src: ImgChoiceFaculty2,
        namaDosen: "Indah Saritem, S. AK, M.Ak",
        tipematkul: 4,
        pertemuan: 14,
        kodematkul: 1142,
        sks: 131,
        slug: "/software-engineering",
      },
    ],
  });

export const queryOptionCourse = atom({
    key: "query-option-course",
    default: "",
  });
  
  export const filterOptionCourse = selector({
    key: "filter-option-course",
    get: ({ get }) =>
      get(optionCourseState).filter((item) =>
        item.namaDosen.toLowerCase().includes(get(queryOptionCourse).toLowerCase()),
      ),
  });