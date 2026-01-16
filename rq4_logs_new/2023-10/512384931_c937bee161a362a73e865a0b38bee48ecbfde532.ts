import {useEffect, useState} from 'react';
import {toast as toaster, useToasterStore, ToastOptions} from 'react-hot-toast';

export function useToast() {
    // 이 전 실행된 토스트 알림과 같은 알림인지 확인하는 state
    const [activeIds, setActiveIds] = useState<string[]>([]);

    const active = (id: string) => setActiveIds((ids) => [...ids, id]);
    const inactive = (id: string) => setActiveIds((ids) => ids.filter((_id) => _id !== id));

    const activeScope = (id: string, logic: () => any, duration = 4000) => {
        if (activeIds.includes(id)) return;
        active(id);
        logic();
        setTimeout(() => inactive(id), duration);
    };

    const info = (msg: string, id?: string) => {
        id ||= msg;
        const duration = 4000;
        activeScope(id, () => {
            toaster(msg, {id, icon: 'ℹ️', duration});
        });
    };

    const success = (msg: string, id?: string) => {
        id ||= msg;
        const duration = 2000;
        activeScope(id, () => toaster.success(msg, {id, duration}), 2000);
    };

    const error = (msg: string, id?: string) => {
        id ||= msg;
        const duration = 4000;
        activeScope(id, () => toaster.error(msg, {id, duration}));
    };

    return {
        toast: {info, success, error},
    };
}