export class Application {
    constructor(
        public id:Number,
        public userId:Number,
        public jobId:Number,
        public appliedOn:String,
        public resumeLink:String


    ){
        this.appliedOn=appliedOn ||"";
        this.id=id ||0;
        this.userId=userId ||0;
        this.resumeLink=resumeLink ||"";
        this.jobId=jobId ||0;
    }
}