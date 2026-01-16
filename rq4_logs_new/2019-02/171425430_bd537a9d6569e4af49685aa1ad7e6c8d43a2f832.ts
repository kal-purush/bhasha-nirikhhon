import { EvalDataSource } from "src/app/datasource/EvalDataSource";
import { TargetService } from "src/app/target-service/target.service";
import { DataStatus } from "src/app/entity/DataStatus";

export class TargetDataSource extends EvalDataSource{
    constructor(public targetService:TargetService){
         super(targetService)
    }
    public  dataStatus = new DataStatus();
    public  dataStatus1 = new DataStatus();
    public  dataStatus2 = new DataStatus();
    /**
     * 加载权限是否全部开放
     */
    loadAuthIsOpen(){
        this.targetService.getAuthIsOpen().subscribe(res=>{
            res.map(
                val=>{
                   this.dataStatus1.anyData= val.data;
                }
            )
        })
    }
    /**
     * 加载所有的时间节点
     */
    loadAllDatePoint(updateMap:Map<string,any>,curList:Array<any>){
        this.dataStatus.loadingEvalSubject.next(true);
        this.targetService.getAllDatePoint().subscribe(res=>{
            res.map(result=>{
                this.dataStatus.pageInfo=result.data.pageInfo;
                //判断是否有修改过的内容
                var dataArray =  this.checkUpdate(updateMap,curList,result.data.listData);
                this.dataStatus.evalMgrSubject.next(dataArray);
                this.dataStatus.myData=dataArray;
                console.log("loadAllDatePoint-----")
                console.log(result.data.listData);
                this.dataStatus.length=result.data.listData.length;
                this.dataStatus.loadingEvalSubject.next(false);
            })
        })
    }


}