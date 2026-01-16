export interface ReferenceDataFlagType {
  name: string;
  name_cy: string;
  hearingRelevant: boolean;
  flagComment: boolean;
  defaultStatus: string | undefined;
  externallyAvailable: boolean;
  flagCode: string;
  isParent: boolean;
  // Note: property is deliberately spelt "Path" and not "path" because the Reference Data Common API returns the former
  Path: string[];
  childFlags: ReferenceDataFlagType[];
  listOfValuesLength: number;
  listOfValues: { key: string; value: string }[];
}

interface flagResourceType {
  [index: number]: string;
}

export interface ReferenceData {
  getFlags(
    accessToken: string,
    serviceId: string,
    flagType: flagResourceType,
    welsh: boolean
  ): Promise<ReferenceDataFlagType>;
}