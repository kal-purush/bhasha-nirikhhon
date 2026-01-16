function movePlayer() {
/*if (player.dx!=0){
    player.x+=(player.dx);
}
if (player.dy!=0){
    player.y+=(player.aim.y);
}
*/
    var pvector=player.aim.normalized().times(6)
    player.x+=pvector.x
    player.y+=pvector.y

    switch(player.aim){
    case Qt.vector2d(0,-1):
        player.animation="from"
        player.horizontalMirror= false
        break;
    case Qt.vector2d(0,1):
        player.animation="to"
        player.horizontalMirror= false
        break;
    case Qt.vector2d(1,0):
        player.animation="r"
        player.horizontalMirror= false
        break;
    case Qt.vector2d(1,-1):
        player.animation="fromr"
        player.horizontalMirror= false
        break;
    case Qt.vector2d(1,1):
        player.animation="tor"
        player.horizontalMirror= false
        break;
    case Qt.vector2d(-1,-1):
        player.animation="froml"
        player.horizontalMirror= true
        break;
    case Qt.vector2d(-1,1):
        player.animation="tol"
        player.horizontalMirror= true
        break;
    case Qt.vector2d(-1,0):
        player.animation="l"
        player.horizontalMirror= true
        break;

    default:
        player.animation="standby"
    }
}

function moveEnemy() {
    var dx=player.x-muffin.x
    var dy=player.y-muffin.y
    var pvector=Qt.vector2d(dx,dy).normalized().times(4)
    muffin.x+=pvector.x
    muffin.y+=pvector.y
}