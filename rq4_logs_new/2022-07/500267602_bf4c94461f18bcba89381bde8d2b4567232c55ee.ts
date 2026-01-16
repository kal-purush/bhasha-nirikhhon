import { Member } from "./member-model";

export interface createMemberModel extends Member {}

export namespace createMemberModel {
  export type Response = createMemberModel;
}