
/**
 * 
 * @param value 
 * @param defaultValue 
 * @returns 
 */
export function useCreateDefaultValue(configs: any) {
    const values: { [key: string]: any } = {};
    for (const key in configs) {
        values[key] = configs[key].value;
    }
    return values;
}