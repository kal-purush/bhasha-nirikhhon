import { errorMessage } from "@/constants/error-message";

export function numberToString(value?: number) {
    if (!value) return "";

    const numberAsString = value.toString();

    if (isNaN(value)) throw new Error(errorMessage.invalidNumber.replace("{0}", numberAsString));

    return value.toString();
}

export function stringToNumber(value?: string) {
    if (!value) return 0;

    const parsed = Number(value);

    if (isNaN(parsed)) throw new Error(errorMessage.invalidNumberString.replace("{0}", value));

    return parsed;
}