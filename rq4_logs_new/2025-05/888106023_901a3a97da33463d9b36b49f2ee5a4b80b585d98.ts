import { IRuleDecision } from "@isettingkit/input";
import { IMessageModal } from "../IMessageModal";
import { IRulesFormTextValues } from "../IRulesFormTextValues";

interface IDecisionsForm {
  attentionModal: IMessageModal;
  deleteModal: IMessageModal;
  initialValues: IRuleDecision[];
  labelBusinessRules: string;
  textValuesBusinessRules: IRulesFormTextValues;
  showAttentionModal: boolean;
  decisionTemplateConfig: (
    enumeratorsRules: IRuleDecision,
    nameMoneyDestination: string,
  ) => IRuleDecision | undefined;
  onButtonClick: () => void;
  onPreviousStep: () => void;
  setDecisions: (decisions: IRuleDecision[]) => void;
  revertModalDisplayData: (
    dataDecision: IRuleDecision,
    originalDecision: IRuleDecision,
  ) => void;
  nameMoneyDestination: string;
  setShowAttentionModal: React.Dispatch<React.SetStateAction<boolean>>;
  editDataOption?: boolean;
  normalizeEvaluateRuleData?: IRuleDecision[];
  titleContentAddCard?: string;
  messageEmptyDecisions?: string;
}

export type { IDecisionsForm };