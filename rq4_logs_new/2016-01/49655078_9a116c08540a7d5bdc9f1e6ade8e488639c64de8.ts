class AppleState extends State
{
    private _world:AppleWorld;
    private _debugDraw:AppleDebugDraw;
    
    public constructor()
    {
        super();
    }
    
    public init():void
    {
        this._world = new AppleWorld();
        
        this._debugDraw = new AppleDebugDraw(this._world);
        
        var box0:AppleBody = new AppleBody();
        box0.mass = 10;
        box0.velocity.x = 0.01;
        box0.position.x = 200;
        box0.position.y = 200;
        box0.addShape(new AppleRect(100,100));
        
        this._world.add(box0);
        this.addChild(this._debugDraw);
        
    }
    
    public tick(advancedTime):void
    {
        this._world.tick(advancedTime);
        this._debugDraw.tick(advancedTime);
    }
}