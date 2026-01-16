class Handler extends Error {
    private status_code: number;

    private errors: unknown[] = [];

    constructor(message: unknown, status_code = 400) {
        super('exception');

        this.message = this.extractMessage(message);
        this.status_code = status_code;
    }

    setErrors(errors: unknown[]): Handler {
        this.errors = errors;
        return this;
    }

    getErrors(): unknown[] {
        return this.errors;
    }

    getStatusCode(): number {
        return this.status_code;
    }

    getMessage(): string {
        return this.message;
    }

    private extractMessage(message: any): string {
        if (typeof message === 'string') return message;

        if (typeof message === 'object') { return Object.values(message).join(' '); }

        return 'unsupported message format';
    }
}

export default Handler;