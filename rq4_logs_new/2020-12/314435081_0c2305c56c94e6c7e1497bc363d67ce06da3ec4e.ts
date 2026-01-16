import { Scenario } from "types/Plan";

export interface ScenarioContextData {
  checkedValues: Array<CheckedValue>;
  setCheckedValues: React.Dispatch<React.SetStateAction<CheckedValue[]>>;
  scenariosList: Scenario[];
  setScenariosList: React.Dispatch<React.SetStateAction<Scenario[]>>;
  scenariosHistory: Scenario[];
  setScenariosHistory: React.Dispatch<React.SetStateAction<Scenario[]>>;
  verifyIfScenariosHistoryHasValue: (
    attr: keyof Scenario,
    value: any,
  ) => boolean;
  handleCheckItem: (data: HandleCheckItem) => void;
  scenarioTitle: string;
  setScenarioTitle: React.Dispatch<React.SetStateAction<string>>;
  disabledColumnsCheckbox: {
    address: boolean;
    threat: boolean;
    hypothese: boolean;
    risk: boolean;
    measure: boolean;
    responsible: boolean;
  };
  handleAddValueToScenario: (data: HandleAddValueToScenario) => any;
  verifyIfIsChecked: (data: VerifyIfIsChecked) => boolean;
  scenarioSaveEnabled: boolean;
  setScenarioSaveEnabled: React.Dispatch<React.SetStateAction<boolean>>;
  generateMergeKey: () => number;
  handleRemoveItem: ({
    attr,
    value,
    rowId,
    rowIndex,
  }: HandleRemoveItem) => void;
}

export interface CheckedValue {
  attr: keyof Scenario;
  value: any;
  rowId: string;
}

export interface HandleAddValueToScenario {
  attr: keyof Scenario;
  value: any;
}

export interface VerifyIfIsChecked {
  attr: keyof Scenario;
  value: any;
  rowId?: string;
  compareMode: "rowId" | "mergeKey";
}

export interface HandleCheckItem {
  attr: keyof Scenario;
  value: any;
  rowId?: string;
  rowIndex?: number;
}

export interface HandleRemoveItem {
  attr: keyof Scenario;
  value: any;
  rowId?: string;
  rowIndex?: number;
}

export interface GetIndexesForMergedLines {
  attr: keyof Scenario;
  startIndex?: number;
  isAdding: boolean;
}

export interface AddInitialScenarioLines {
  attr: keyof Scenario;
  value: any;
  rowId: string;
}