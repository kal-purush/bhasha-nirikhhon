import create from "zustand";

interface OnBoardingSurvey {
    id: number;
    title: string;
    subtitle: string;
    candidates: string[];

}

interface OnBoardingResult {
    gender: number;
    ageGroup: number;
    symptoms: string[];
    purpose: number[];
    setGender: (answer: number) => void;
    setAgeGroup: (answer: number) => void;

}


export const onBoardingSurveys: OnBoardingSurvey[] = [

    {
        id: 0,
        title: "성별을 입력해주세요.",
        subtitle: "쿠잇 님의 개인정보에 맞는 건강 정보를 알려드려요",
        candidates: ["여성", "남성"],
    },
    {
        id: 1,
        title: "연령대를 선택해주세요",
        subtitle: "",
        candidates: ["40대 이하", "50대", "60대", "70대 이상"],
    }

]

interface OnBoardingState {
    currentQuestionIdx: number;
    nextQuestion: () => void;
}

export const useOnBoardingSurveyStore = create<OnBoardingState>((set) => ({
    currentQuestionIdx: 0,
    nextQuestion: () => set((state) => ({ currentQuestionIdx: state.currentQuestionIdx + 1 })),
}));

export const OnBoardingResult = create<OnBoardingResult>((set) => ({
    gender: -1,
    ageGroup: -1,
    symptoms: [],
    purpose: [],
    setGender: (answer: number) => {
        console.log("ㄱㄷ녀ㅣㅅㅁ;ㅇ널")
        set({ gender: answer });
    },
    setAgeGroup: (answer: number) => set({ ageGroup: answer }),
}));


