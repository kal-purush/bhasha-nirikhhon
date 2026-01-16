class P2State extends State
{
    private _p2Engine:P2Engine;
    private _debugDraw:p2DebugDraw;
    
    private _hero:P2Hero;
    
    public constructor()
    {
        super();
    }
    
    public init():void
    {
        this._p2Engine = new P2Engine("p2");
        
        this._debugDraw = new p2DebugDraw(this._p2Engine.world);
        
        var sprite: egret.Sprite = new egret.Sprite();
        this.addChild(sprite);
        this._debugDraw.setSprite(sprite);
        
        
        var b:P2PhysicsObject = new P2PhysicsObject("a",{p2:this._p2Engine, width:100,height:100})
        b.x = 100;
        b.y = 100;
        b.velocity = [10,10]
        
        //this._p2Engine.add(b);
        
        var p0:P2Platform = new P2Platform("ground",{p2:this._p2Engine, width:400,height:20});
        p0.y = 400;
        p0.x = 200;
        
        this._p2Engine.add(p0);
        
        this._hero = new P2Hero("hero",{width:50,height:100});
        this._hero.y = 200;
        this._hero.x = 200;
        this._p2Engine.add(this._hero);
        
        var that = this;
        document.addEventListener("keydown", function onKeydown(e:KeyboardEvent){
            if(that._hero == null)
            {
                return;
            }
            switch (e.keyCode) {
                case 37: //left
                    if(!that._hero.isDoing("left"))
                    {
                        that._hero.setIsDoing("left",true);
                    }
                    break;
                case 39: //right
                    if(!that._hero.isDoing("right"))
                    {
                        that._hero.setIsDoing("right", true);
                    }
                    break;
                case 38: //up
                    if(!that._hero.isDoing("jump") && !that._hero.justDid("jump"))
                    {
                        that._hero.setJustDid("jump", true);
                    }
                    else if(!that._hero.isDoing("jump") && that._hero.justDid("jump"))
                    {
                        
                        that._hero.setIsDoing("jump",true);
                        that._hero.setJustDid("jump", false);
                    }
                    break;
                case 40: //down
                    break;
                default:
                    break;
            }
        });
        
        
        document.addEventListener("keyup", function onKeyup(e:KeyboardEvent)
        {
            if(that._hero == null)
            {
                return;
            }
            switch (e.keyCode) {
                case 37: //left
                    that._hero.setIsDoing("left",false);
                    that._hero.setJustDid("left", false);
                    break;
                case 39: //right
                    that._hero.setIsDoing("right",false);
                    that._hero.setJustDid("right", false);
                    break;
                case 38: //up
                    that._hero.setIsDoing("jump",false);
                    that._hero.setJustDid("jump", false);
                    break;
                case 40: //down
                    break;
                default:
                    break;
            }
        });
    }
    
    
    
    public tick(advancedTime):void
    {
        this._p2Engine.tick(advancedTime);
        this._debugDraw.drawDebug();
    }
}