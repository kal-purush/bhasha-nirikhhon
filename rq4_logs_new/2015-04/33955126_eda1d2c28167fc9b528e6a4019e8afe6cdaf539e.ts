
interface brickDataConfig {
    brickEl: any;
}

class Brick {

    $baseObjRef: Base;

    $brick: brickDataConfig;

    constructor( base: Base ) {
        this.$baseObjRef = base;

        this.$brick = {
            brickEl: null
        };

        this.drawBrick();
    }

    drawBrick() {
        var $base = this.$baseObjRef.$base;

        var baseRadius = this.$baseObjRef.$field.radius;
        var angle = $base.startAngle;
        var edgesNum = $base.edgesNum;
        var brickPath: string;

        var x = $base.baseX + baseRadius * Math.cos(Math.PI * angle / 180);
        var y = $base.baseY + baseRadius * Math.sin(Math.PI * angle / 180);
        brickPath = "M " + String(x) + "," + String(y) + " ";

        angle += 360 / edgesNum;
        brickPath += nextLine(baseRadius, angle);

        baseRadius -= 20;
        brickPath += nextLine(baseRadius, angle);

        angle -= 360 / edgesNum;
        brickPath += nextLine(baseRadius, angle);

        this.$brick.brickEl = this.$baseObjRef.$gamePaper.path( brickPath );
        console.log( this.$brick.brickEl );
        this.$brick.brickEl.node.setAttribute( 'class', 'brick ygreen' );

        function nextLine(baseRadius, angle) {
            x = $base.baseX + baseRadius * Math.cos(Math.PI * angle / 180);
            y = $base.baseY + baseRadius * Math.sin(Math.PI * angle / 180);
            return "L " + String(x) + "," + String(y) + " ";
        }
    }

}