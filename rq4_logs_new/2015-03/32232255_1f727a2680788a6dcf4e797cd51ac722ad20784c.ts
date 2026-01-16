///<reference path="./node.d.ts" />
///<reference path="./util.d.ts" />
///<reference path="../node_modules/my-user/lib.d.ts" />

import extend=require('extend');

import user=require('./user');

import User=user.User;
import UserConfig=user.UserConfig;
import UserDoc=user.UserDoc;
import UserCallback=user.UserCallback;

interface ManagerOptions{
    user?:user.ManagerOptions;
}

interface DBOptions{
    db?:any;
    url?:string;
    collection?:{
        user?:string;
        ticket?:string;
    };
}

interface EntryResult{
    user:User;
}

export class Manager{
    public userconfig:UserConfig;
    private db:any;
    private collection:{
        user:string;
        ticket:string;
    };
    private options:ManagerOptions;

    public user:user.Manager;

    constructor(options?:any){
        this.options=options || {};
        this.userconfig=user.init();
    }

    //initialize
    init(options:DBOptions,callback:Callback):void{
        //default values
        options=extend(true,{
            collection:{
                user:"user",
                ticket:"ticket"
            }
        },options);
        
        //Error check
        if(options.db==null && options.url==null){
            throw new Error("Neither Db instance nor connection url is given.");
        }
        if(options.db==null && callback==null){
            throw new Error("Callback function is required when connection url is specified.");
        }

        //Read options
        this.collection = {
            user: options.collection.user,
            ticket: options.collection.ticket
        };

        //Connect to db
        if(options.db!=null){
            //already active db is given
            this.db=options.db;
            this.initComponents();
            if(callback){
                callback(null,options.db);
            }
        }else {
            //newly connect
            require('./db').connect(options.url,(err,db)=>{
                if(err){
                    callback(err,null);
                    return;
                }
                this.db=db;
                this.initComponents();
                callback(null,db);
            });
        }
    }
    private initComponents():void{
        //db
        this.user=new user.Manager(this.userconfig,this.db,this.collection.user,this.options.user);
    }
    //high level user manipulation
    entry(data:any,callback:(err,result:EntryResult)=>void):void{
        this.user.createNewUser(data,(err,u:User)=>{
            if(err){
                callback(err,null);
                return;
            }
            callback(null,{
                user:u
            });
        });
    }
}