export class Argument {
    public key: string;
    public value: string;
}

export class ArgBuilder {

    private args: string[] = [];
    
    public add(arg: Argument): void
    {
        if (arg.value !== null)
        {
            this.args[arg.key] = arg.value;
        }
    }

    public addRange(range: Argument[]): void
    {
        for(let obj in range) {
            this.add(range[obj]);
        }
    }

    public get(key: string): string
    {
        return this.args[key];
    }

    public generate(): string
    {
        let result: string[] = [];
        Object.keys(this.args).forEach(function(key){
            result.push(`--${key}=${this.args[key]}`);
        }, this);
        return result.join(' ');
    }
}