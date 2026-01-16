package hansung.ElderCare.Server.apiPayload.exception;

import hansung.ElderCare.Server.apiPayload.code.BaseErrorCode;

public class DeviceHandler extends GeneralException {
    public DeviceHandler(BaseErrorCode code) {super(code);}
}