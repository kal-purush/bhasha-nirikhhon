interface IOptionParameter<T> {
    name: string;
    label: string;
    description: string;

    defaultValue: T;
    min?: T;
    max?: T;
    step?: number;
}

export default abstract class OptionParameter<T> implements IOptionParameter<T> {
    static registred = new Map<string, OptionParameter<unknown>>();
    
    public readonly label!: string;
    public readonly name!: string;
    public readonly description!: string;

    public readonly defaultValue!: T;
    public readonly min?: T | undefined;
    public readonly max?: T | undefined;
    public readonly step?: number;

    constructor(parameter: IOptionParameter<T>) {
        const registred = OptionParameter.registred.get(parameter.name)
        if (registred) {
            throw new Error(`"${parameter.name}" option parameter already registred.`)
        }

        Object.assign(this, parameter)
        OptionParameter.registred.set(parameter.name, this)
    }

    abstract fromRaw(rawValue: string): T;
}