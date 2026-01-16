class Utils {
    public static checkCollision(n : GameObject, m : GameObject): boolean {
        return (n.x < m.x + m.width &&
            n.x + n.width > m.x &&
            n.y < m.y + m.height &&
            n.height + n.y > m.y)
    }
    
    public static getRandom(min : number, max : number): number{
        return Math.floor(Math.random() * ( max - min )) + min;
    }
}