import { ISpinnerAppearance } from "@inubekit/inubekit";
import { ISaveDataResponse } from "@ptypes/saveData/ISaveDataResponse";
import { IRequestSteps } from "../IRequestSteps";

interface IRequestProcessContent {
  descriptionRequestProcess: {
    title: string;
    description: string;
  };
  portalId: string;
  requestProcessSteps: IRequestSteps[];
  descriptionRequestStatus: (responsible: string) => {
    actionText: string;
    description: string;
    title: string;
  };
  onCloseRequestStatus: () => void;
  saveData?: ISaveDataResponse;
  appearance?: ISpinnerAppearance;
}

export type { IRequestProcessContent };