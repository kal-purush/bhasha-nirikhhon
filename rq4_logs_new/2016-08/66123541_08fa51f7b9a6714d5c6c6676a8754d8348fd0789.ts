// Generic Object class to represent game entities
class Entity{
    constructor(){

    }

    /** function called once, when the object is created */
    public start() {};

    /**function called every frame */
    public update() {};

    /**
    *Draws object's sprite to screen
    *@param stage The stage this objject should be drawn to
    */
    public draw(stage) {};

    /** function called when object is destroyed*/
    public destroy() {};
}