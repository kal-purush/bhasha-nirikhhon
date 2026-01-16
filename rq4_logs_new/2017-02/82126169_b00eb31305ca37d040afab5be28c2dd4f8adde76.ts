import {
    Message
} from "./message";
export const MessageDataOKMock: Message = {
        "status": "ok",
        "text": "This message should not be shown",
        "cssClass": "ok",
        "icon" : "-ok-sign",
};

export const MessageDataMessageMock: Message = {
        "status": "message",
        "text": "This message should be shown",
        "cssClass": "info",
        "icon" : "-info-sign",
};

export const MessageDataMaintenanceMock: Message = {
        "status": "ok",
        "text": "This message needs maintenance",
        "cssClass": "warning",
        "icon" : "-wrench",
};

export const MessageDataErrorMock: Message = {
        "status": "error",
        "text": "This is an error",
        "cssClass": "danger",
        "icon" : "-warning-sign",
};

export const MessageDataOtherMock: Message = {
        "status": "other",
        "text": "This message is unknown",
        "cssClass": "other",
        "icon" : "",
};