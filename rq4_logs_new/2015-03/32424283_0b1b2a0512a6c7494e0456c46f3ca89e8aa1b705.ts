module RC {

    export declare class SDK {

        static version:string;

        protected platform:RC.platform.Platform;

        constructor(appKey:string, $appSecret:string, $server:string);

        getPlatform():RC.platform.Platform;

        getSubscription():RC.subscription.Subscription;

    }
}