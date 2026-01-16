import { useState } from 'react';

export class ClipboardService {
    public copy(value?: string) {
        return new Promise<void>((resolve) => {
            const el = document.createElement('textarea');
            el.value = value;
            el.setAttribute('readonly', '');
            el.style.position = 'absolute';
            el.style.left = '-9999px';
            document.body.appendChild(el);
            el.select();
            navigator.clipboard
                .writeText(value)
                .then(() => {
                    document.body.removeChild(el);
                    resolve();
                })
                .catch((err) => {
                    console.log(err);
                });
        });
    }
}

export function useClipboardService(): ClipboardService {
    return useState(new ClipboardService())[0];
}