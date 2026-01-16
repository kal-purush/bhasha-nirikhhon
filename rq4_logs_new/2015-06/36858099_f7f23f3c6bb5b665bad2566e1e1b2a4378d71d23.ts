module GrowthAnalytics {

    export class ClientEvent {

        private id:string;
        private clientId:string;
        private eventId:string;
        // FIXME data type
        private properties:any;
        private created:Date;

        constructor(data?:any) {

            if (data == undefined)
                return;

            this.id = data.id;
            this.clientId = data.clientId;
            this.eventId = data.eventId;
            this.properties = data.properties;
            // FIXME DateUtils.foramt();
            this.created = data.created;

        }

        public static create(clientId:string, eventId:string, properties:any, credentialId:string, success:(client:ClientEvent)=>void, failure:(error:any)=>void):void {

            // FIXME merge GrowthbeatCore
            nanoajax.ajax({
                url: 'https://api.analytics.growthbeat.com/1/clients/',
                method: 'POST',
                body: 'clientId=' + clientId
                + '&eventId=' + eventId
                + '&properties=' + properties
                + '&credentialId=' + credentialId
            }, (code:number, responseText:string)=> {
                if(code !== 200)
                    failure('failure');
                success(new ClientEvent(JSON.parse(responseText)));
            });

        }

        public getId():string {
            return this.id;
        }

        public setId(id:string):void {
            this.id = id;
        }

        public getClientId():string {
            return this.clientId;
        }

        public setClientId(clientId:string):void {
            this.clientId = clientId;
        }

        public getEventId():string {
            return this.eventId;
        }

        public setEventId(eventId:string):void {
            this.eventId = eventId;
        }

        public getProperties():any {
            return this.properties;
        }

        public setProperties(properties:any):void {
            this.properties = properties;
        }

        public getCreated():Date {
            return this.created;
        }

        public setCreated(created:Date):void {
            this.created = created;
        }

    }

}