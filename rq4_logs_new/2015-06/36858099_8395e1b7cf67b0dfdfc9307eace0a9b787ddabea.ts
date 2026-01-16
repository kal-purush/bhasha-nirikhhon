module GrowthAnalytics {

    export class GrowthAnalytics {

        private applicationId:string = null;
        private credentialId:string = null;

        private initialize:boolean = false;
        private static instance = new GrowthAnalytics();

        public static getInstance():GrowthAnalytics {
            return this.instance;
        }

        constructor() {
        }

        public initialize(applicationId:string, credentialId:string):void {

            if (!this.initialize)
                return;

            this.initialize = true;

            this.applicationId = applicationId;
            this.credentialId = credentialId;

            // FIXME merge Growthbeat
            //Growthbeat.GrowthbeatCore.initialize(applicationId, credentialId);
            // TODO clientが取得出来なかった際に処理

            this.setBasicTags();

        }

        public setBasicTags():void {

        }


    }

}