class AppleBody
{
    public static DYNAMIC:string = "dynamic";
    public static KINEMATIC:string = "kinematic";
    public static STATIC:string = "static";
    
    
    public mass : number = 1;
    public position : egret.Point;
    public rotation : number = 0;
    public shapes : Array<any>;
    public world: AppleWorld;
    public type : string;
    public velocity : egret.Point;
    
    protected _force : egret.Point;
    protected _acceleration:egret.Point;
    protected _isDynamic:boolean;
    protected _isKinematic:boolean;
    protected _isStatic:boolean;
    
    public constructor(type:string = AppleBody.DYNAMIC, position:egret.Point = new egret.Point())
    {
        this.type = type;
        switch(this.type)
        {
            case AppleBody.STATIC:
                 this._isStatic = true;
                 break;
            case AppleBody.DYNAMIC:
                 this._isDynamic = true;
                 break;
            case AppleBody.KINEMATIC:
                 this._isKinematic = true;
                 break;
        }
        this.position = position;
        
        this.velocity = new egret.Point();
        this._force = new egret.Point();
        this._acceleration = new egret.Point();
    }
    
    public step(passTime:number):void
    {
        //vt = v0 + at
        this.velocity.x += this._acceleration.x * passTime;
        this.velocity.y += this._acceleration.y * passTime;
        //st = s0 + vt 
        this.position.x = this.velocity.x * passTime;
        this.position.y = this.velocity.y * passTime;
    }
    
    public setVelocity(vx:number, vy:number):void
    {
        this._force.x = vx;
        this._force.y = vy;
        
        this._acceleration.x = this._force.x / this.mass;
        this._acceleration.y = this._force.y / this.mass;
    }
    public get bounds () : egret.Rectangle
    {
        return null;
    }
    
    public get isSleeping () : boolean
    {
        return false;
    }

    public applyImpulse (impulse:egret.Point):void
    {
        //fs =mv;
        
    }

    public isDynamic() : boolean
    {
        return this._isDynamic;
    }

    public isKinematic() : boolean
    {
        return this._isKinematic;
    }

    public isStatic() : boolean
    {
        return this._isStatic;
    }

    public rotate (angle:Number):void
    {
        
    }
}