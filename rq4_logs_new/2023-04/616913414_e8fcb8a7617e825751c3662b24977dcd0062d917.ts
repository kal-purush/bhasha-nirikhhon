import { useRecoilState } from "recoil";
import { PrivateInformationState, JobInformationState, FileInformationState, AdministrationStatusState } from "./store";
import { ReturnTypesPrivateInformation, ReturnTypesJobInformation, ReturnTypesFileInformation, StatusReturnTypesAdministration } from "./type";

export const usePrivateInformationStatus = (): ReturnTypesPrivateInformation => {
  const [get, set] = useRecoilState(PrivateInformationState);
  return {
    setPrivateStatus: (val: boolean) => set(val),
    getPrivateStatus: get,
  };
};

export const useJobInformationStatus = (): ReturnTypesJobInformation => {
  const [get, set] = useRecoilState(JobInformationState);
  return {
    setJobStatus: (val: boolean) => set(val),
    getJobStatus: get,
  };
};

export const useFileInformationStatus = (): ReturnTypesFileInformation => {
  const [getStatus, setStatus] = useRecoilState(FileInformationState);
  return {
    setFileStatus: (val: boolean) => setStatus(val),
    getFileStatus: getStatus,
  };
};

export const useAdministrationStatus = (): StatusReturnTypesAdministration => {
  const [getStatus, setStatus] = useRecoilState(AdministrationStatusState);
  return {
    setAdministrationStatus: (val: string) => setStatus(val),
    getAdministrationStatus: getStatus,
  };
};