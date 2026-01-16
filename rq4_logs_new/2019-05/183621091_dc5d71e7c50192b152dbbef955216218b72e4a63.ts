import {DialogflowAgentBuilder} from "../LineBotService/DialogflowAgentBuilder";
import {IDialogflowService} from "./IDialogflowService";
import {IMaintainService} from "../MaintainService/IMaintainService";
import {Message, TextMessage} from "@line/bot-sdk";
import {TYPES} from "../../ioc/types";
import {inject, injectable} from "inversify";

@injectable()
export class DialogflowService implements IDialogflowService {

    public constructor(@inject(TYPES.IMaintainService) private maintainService: IMaintainService) {
    }

    public dispatchMessage(
        userId: string, message: string, lineMessageCallback: (userId: string, message: Message | Array<Message>) => void, errorCallback? : (error) => void) {
        const request = DialogflowAgentBuilder.DialogflowAgent.textRequest(message, {sessionId: userId});

        request.on("response", async response => {

            const action = response.result.action;

            switch(action) {
                case "requestReport":
                    const requestReportMessage = this.maintainService.requestReport(userId);
                    lineMessageCallback(userId, requestReportMessage);
                    break;

                case "searchReport":
                    const searchReportMessage = await this.maintainService.searchReport(userId, response.result);
                    lineMessageCallback(userId, searchReportMessage);
                    break;

                default:
                    const errorMessage: TextMessage = {
                        type: "text",
                        text: (response.result.fulfillment.messages[0].speech as string).replace("{{message}}", response.result.resolvedQuery)
                    };

                    lineMessageCallback(userId, errorMessage);
            }
        }).end();

        request.on("error", error => {
            if (errorCallback) {
                errorCallback(error);
            }
        });
    }
}