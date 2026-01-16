class GradientPoint
{
    position: Number;
    color: string;
    constructor( aPosition: Number, aColor: string)
    {
        if (aPosition < 0.0 || aPosition > 1.0)
            throw new Error("Position not in range [0.0, 1.0]: " + aPosition);
        if (! /^#[0-9a-f]{6,6}$/.test( aColor))
            throw new Error("Color not in form '#xxxxxx': " + aColor);
        this.position = aPosition;
        this.color = aColor;
    }
}

var gradientPoints: GradientPoint[]
{
    new GradientPoint( 0.0, "#660099"),
    new GradientPoint( 0.25, "#3366ff"),
    new GradientPoint( 0.5, "#cceeff"),
    new GradientPoint( 0.501, "#ffffcc"),
    new GradientPoint( 0.675, "#00ff00"),
    new GradientPoint( 0.875, "#996633"),
    new GradientPoint( 1.0, "#ffffff")
}

function loaded()
{
    var tbl : HTMLTableElement;
    tbl = <HTMLTableElement> document.querySelector( "#gradientPointsTable");

    for (var i = 0; i < tbl.children.length; i++)
    {
        var tbody : HTMLTableSectionElement = <HTMLTableSectionElement> tbl.children[i];
        if (tbody.rows)
        {
            while (tbody.rows.length > 3)
                tbody.deleteRow( 1);
        }
    }
}