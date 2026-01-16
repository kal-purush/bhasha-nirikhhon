

function Node (x, y, radius, color) {
    this.x          =   x;
    this.y          =   y;
    this.radius     =   radius || defaultNodeRadius;
    this.color      =   color  || defaultNodeColor;
    this.id         =   ++nodeIdIncrementer;
    this.siblings   =   [];
}

Node.prototype.draw     =   function () {
    try {
        if (!this.x || !this.y) { throw "X, Y location not specified. Cannot draw node." }
        else if (nodeIdList.indexOf(this.id) !== -1) { throw "Node having same id exists. Cannot draw node." }
        else if (doesNodeOverlap(this)) { throw "Node overlaps or touches another node. Cannot draw node." }
        else {
            drawCircle(nodeyContext, this.x, this.y, this.radius, this.color);
            nodeIdList.push(this.id);
            nodeList.push(this);
        }
    } catch (e) {
        if (!nodeyContext) { throwError("No context. Cannot draw node."); }
        else { throwError(e); }
    }
};