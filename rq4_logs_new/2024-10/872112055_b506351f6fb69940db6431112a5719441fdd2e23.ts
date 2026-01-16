declare module '@paystack/inline-js' {
    export interface PaystackOptions {
        key: string;
        email: string;
        amount: number;
        currency?: string;
        ref?: string;
        callback?: (response: any) => void;
        onClose?: () => void;
    }

    interface PaystackPop {
        setup(options: PaystackOptions): {
            openIframe(): void;
        };
    }

    const PaystackPop: PaystackPop;
    export default PaystackPop;
}