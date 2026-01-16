module hh{


    export class ResCfgItem{
        public name:string;
        public url:string;
        public type:string;
        public scale9grid:string;
        public cb:()=>void;
        public ctx:any;
        public logEnabled:boolean = true;
    }

    export var _pool:any = {};//资源缓存池
    export var _resCfg:any = {};//资源配置
    export var _aliasCfg:any = {};//资源别名配置
    export var _counter:any = {};//资源加载计数器
    export var _parserDic:any = {};//资源解析器映射库，key为解析器type。
    export var _parserAliasDic:any = {};//资源解析器类型别名映射，key为别名，value为parserType
    export var _defaultLimit:number = 100;//并行加载个数

    export var resRoot:string = "";//资源根路径

    export function loadImage(url:string, option?:any, cb?:any){
        var opt = {
            isCrossOrigin : true
        };
        if(cb !== undefined) {
            opt.isCrossOrigin = option.isCrossOrigin == null ? opt.isCrossOrigin : option.isCrossOrigin;
        }
        else if(option !== undefined)
            cb = option;

        var img = new Image();
        if(opt.isCrossOrigin)
            img.crossOrigin = "Anonymous";

        img.addEventListener("load", function () {
            this.removeEventListener('load', arguments.callee, false);
            this.removeEventListener('error', arguments.callee, false);
            if(cb)
                cb(null, img);
        });
        img.addEventListener("error", function () {
            this.removeEventListener('error', arguments.callee, false);
            if(cb)
                cb("load image failed");
        });
        img.src = url;
        return img;
    }
}