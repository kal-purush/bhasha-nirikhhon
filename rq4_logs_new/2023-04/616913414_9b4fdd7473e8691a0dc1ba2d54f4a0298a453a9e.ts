import { useRecoilState } from "recoil";
import { CategorySimulationTypes, ScheduleTypes, ChooseSimulationTypes, ReturnTypesPopupScheduleStatus, ChooseTimeSimulasiTypes } from "./type";
import { categorySimulationState, chooseScheduleState, chooseSimulasiState, PopupModalScheduleOpen, ChooseTimeSimulasiState, CheckRescheduleSimulationState } from "./store";

export const useCategorySimulation = (): CategorySimulationTypes => {
    const [getCategory, setCategory] = useRecoilState(categorySimulationState);
    return{
        setCategorySimulation: (val: string) => setCategory(val),
        getCategorySimulation: getCategory,
    };
};

export const useScheduleSimulation = (): ScheduleTypes => {
    const [getSchedule, setSchedule] = useRecoilState(chooseScheduleState);
    return{
        setScheduleSimulation: (val: boolean) => setSchedule(val),
        getScheduleSimulation: getSchedule,
    };
};

export const useChooseSimulation = (): ChooseSimulationTypes => {
    const [getChoose, setChoose] = useRecoilState(chooseSimulasiState);
    return{
        setChooseSimulation: (val: string) => setChoose(val),
        getChooseSimulation: getChoose,
    };
};

export const usePopupScheduleStatus = (): ReturnTypesPopupScheduleStatus => {
  const [get, set] = useRecoilState(PopupModalScheduleOpen);
  return {
    setPopupStatus: (val: boolean) => set(val),
    getPopupStatus: get,
  };
};

export const useChooseTimeSimulation = (): ChooseTimeSimulasiTypes =>{
    const [getTimeReschedule, setTimeReschedule] = useRecoilState(ChooseTimeSimulasiState);
    return{
        setChooseTimeSimulation: (val: string) => setTimeReschedule(val),
        getChooseTimeSimulation: getTimeReschedule,
    };
};
import { RescheduleSimulation } from "@/stores/Simulation/type";


type CheckRescheduleReturnTypes = {
    setCheckResheduleSimulation: (val: Array<RescheduleSimulation>) => void;
    getCheckRescheduleSimulation: Array<RescheduleSimulation>;
}

export const useCheckRescheduleSimulation = (): CheckRescheduleReturnTypes => {
  const [getCheckReschedule, setCheckReschedule] = useRecoilState(
    CheckRescheduleSimulationState
  );
  return {
    setCheckResheduleSimulation: (val:  Array<RescheduleSimulation>) => setCheckReschedule(val),
    getCheckRescheduleSimulation: getCheckReschedule,
  };
};