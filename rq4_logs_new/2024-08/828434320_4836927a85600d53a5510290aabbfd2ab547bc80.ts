import { create } from "zustand";

interface viewingChallengeState {
    viewingChallenge: string;
    setViewingChallenge: (type: string) => void;
}


const useViewingChallengeStore = create<viewingChallengeState>((set) => {

  return {
    viewingChallenge: "pill",
    setViewingChallenge: (type: string) => set({viewingChallenge: type})
  };
});

export default useViewingChallengeStore;