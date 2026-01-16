import { ApprovalStatus } from "$interfaces/ApprovalStatus";
import { IButtonProps } from "$components/Button";
import { FunctionComponent } from "react";
import { SvgIconProps } from "@material-ui/core/SvgIcon";

export interface IContainer extends IButtonProps {
  Icon: FunctionComponent<SvgIconProps>;
  setStatus: (status: ApprovalStatus) => void;
  status: ApprovalStatus;
}

export interface ITranslations {
  approve: string;
  reject: string;
  pending: string;
}

export interface IComponent extends IContainer {
  label: string;
}