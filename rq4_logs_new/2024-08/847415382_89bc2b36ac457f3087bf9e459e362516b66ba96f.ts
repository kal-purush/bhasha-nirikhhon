// src/global.d.ts

declare global {
    interface Window {
        MathJax: {
            typesetPromise: () => Promise<void>;
            startup: {
                promise: Promise<void>;
                typeset: boolean;
            };
            // You can add more properties as needed
        };
    }
}