import { BigNumber } from "ethers";

export class Utils {
    /**
     * Check whether the string is a integer.
     * @param value
     */
    public static isInteger(value: string): boolean {
        return /^[+-]?([0-9]+)$/.test(value);
    }

    /**
     * Check whether the string is a positive integer.
     * @param value
     */
    public static isPositiveInteger(value: string): boolean {
        return /^(\+)?([0-9]+)$/.test(value);
    }

    /**
     * Check whether the string is a negative integer.
     * @param value
     */
    public static isNegativeInteger(value: string): boolean {
        return /^-([0-9]+)$/.test(value);
    }

    /**
     * Check whether the string is a positive.
     * @param value
     */
    public static isPositive(value: string): boolean {
        return /^(\+)?[0-9]\d*(\.\d+)?$/.test(value);
    }

    /**
     * Check whether the string is a negative.
     * @param value
     */
    public static isNegative(value: string): boolean {
        return /^-?[0-9]\d*(\.\d+)?$/.test(value);
    }

    public static isAmount(value: string): Promise<string> {
        return new Promise<string>((resolve, reject) => {
            if (!Utils.isPositiveInteger(value)) {
                return reject(new Error("Invalid value"));
            }
            try {
                BigNumber.from(value);
            } catch (e) {
                return reject(new Error("Invalid value"));
            }
            return resolve(value);
        });
    }
}