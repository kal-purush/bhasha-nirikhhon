declare module 'nprogress' {
    interface NProgressOptions {
        minimum?: number;
        easing?: string;
        speed?: number;
        trickle?: boolean;
        trickleSpeed?: number;
        showSpinner?: boolean;
        parent?: string;
    }

    interface NProgress {
        configure: (options: NProgressOptions) => NProgress;
        start: () => NProgress;
        done: (force?: boolean) => NProgress;
        inc: (amount?: number) => NProgress;
        set: (amount: number) => NProgress;
        remove: () => void;
        isRendered: () => boolean;
        getPositioningCSS: () => string;
    }

    const nprogress: NProgress;
    export default nprogress;
}