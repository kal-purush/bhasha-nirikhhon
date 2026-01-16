import { MessageResponse } from "../../model/message-response";

export interface StatusModel {
   isLoading: boolean;
   responseObjects: MessageResponse[];
}