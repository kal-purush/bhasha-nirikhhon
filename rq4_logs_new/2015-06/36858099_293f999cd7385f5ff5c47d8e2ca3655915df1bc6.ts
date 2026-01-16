module GrowthAnalytics {

    export class ClientTag {

        private clientId:string;
        private tagId:string;
        // FIXME data type
        private value:string;
        private created:Date;

        constructor(data?:any) {

            if (data == undefined)
                return;

            this.clientId = data.clientId;
            this.tagId = data.tagId;
            this.value = data.value;
            // FIXME DateUtils.foramt();
            this.created = data.created;

        }

        public static create(clientId:string, tagId:string, value:string, credentialId:string, success:(clientTag:ClientTag)=>void, failure:(error:any)=>void):void {

            // FIXME if value is null
            // FIXME merge GrowthbeatCore
            nanoajax.ajax({
                url: 'https://api.analytics.growthbeat.com/1/clients/',
                method: 'POST',
                body: 'clientId=' + clientId
                + '&tagId=' + tagId
                + '&value=' + value
                + '&credentialId=' + credentialId
            }, (code:number, responseText:string)=> {
                if(code !== 200)
                    failure('failure');
                success(new ClientTag(JSON.parse(responseText)));
            });

        }

        public getClientId():string {
            return this.clientId;
        }

        public setClientId(clientId:string):void {
            this.clientId = clientId;
        }

        public getTagId():string {
            return this.tagId;
        }

        public setTagId(tagId:string):void {
            this.tagId = tagId;
        }

        public getValue():string {
            return this.value;
        }

        public setValue(value:string):void {
            this.value = value;
        }

        public getCreated():Date {
            return this.created;
        }

        public setCreated(created:Date):void {
            this.created = created;
        }

    }

}