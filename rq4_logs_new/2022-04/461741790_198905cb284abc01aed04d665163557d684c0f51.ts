import { DBCtrler } from "../firestore/DBCtrler";
import { TStationDocument, TTimetableDocument, TLineDocument } from "../firestore/DBCtrler.types";
import { firestore } from "../firestore/firebaseApp";

export interface State {
  dbCtrler: DBCtrler | undefined,

  lineData: TLineDocument | undefined,
  lineDataId: string

  trainData: TTimetableDocument | undefined,
  trainDataId: string,

  stations: Map<string, TStationDocument> | undefined,
}

export const intiialState: State = {
  dbCtrler: new DBCtrler(firestore),

  lineData: undefined,
  lineDataId: "",

  trainData: undefined,
  trainDataId: "",

  stations: undefined
};