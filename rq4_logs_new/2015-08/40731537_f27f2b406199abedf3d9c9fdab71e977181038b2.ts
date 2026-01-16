

var vertexColorsAll = [
    [1.0, 0.0, 0.0, 1.0],  // red
    [1.0, 1.0, 0.0, 1.0],  // yellow
    [0.0, 1.0, 0.0, 1.0],  // green
    [0.0, 0.0, 1.0, 1.0],  // blue
    [1.0, 0.0, 1.0, 1.0],  // magenta
    [0.0, 1.0, 1.0, 1.0],  // cyan
    [1.0, 1.0, 1.0, 1.0],   // white
    [0.0, 0.0, 0.0, 1.0]  // black
];


function GetColorsArray(length: number): number[][]
{
    var colors: number[][] = []
    for (var i = 0; i < length; ++i) {
        var r = Math.floor(Math.random() * 5); //0 to 5
        //colors.push(vec4(1, 0, 0, 1))
        colors.push(vertexColorsAll[r])
    }
    return colors
}


function CreateCube(size: number)
{
    size = size * 0.5

    var vertices = [
        vec4(-size, -size, size, 1.0),
        vec4(-size, size, size, 1.0),
        vec4(size, size, size, 1.0),
        vec4(size, -size, size, 1.0),
        vec4(-size, -size, -size, 1.0),
        vec4(-size, size, -size, 1.0),
        vec4(size, size, -size, 1.0),
        vec4(size, -size, -size, 1.0)
    ];

    var indices = [
        [1, 0, 3],
        [3, 2, 1],
        [2, 3, 7],
        [7, 6, 2],
        [3, 0, 4],
        [4, 7, 3],
        [6, 5, 1],
        [1, 2, 6],
        [4, 5, 6],
        [6, 7, 4],
        [5, 4, 0],
        [0, 1, 5]
    ];

    var result =
        {
            triangles: indices,
            vertices: vertices,
        }

    return result
}


function CreateCylinder(size : number)
{
    var r = 0.7 * size
    var h = 0.7 * size
    var segmentCount = 20

    var verticesCircle1 = []
    var verticesCircle2 = []

    var deltaTheta = 360 / segmentCount  //zx plane
    for (var theta = 0; theta < 360; theta += deltaTheta)
    {
        var theta_ = radians(theta)
        var x = r * Math.cos(theta_)
        var z = r * Math.sin(theta_)
        verticesCircle1.push(vec4(x, h, z, 1))
        verticesCircle2.push(vec4(x, -h, z, 1))
    }

    var vertexCenter1: any = vec4(0, h, 0, 1)
    var vertexCenter2: any = vec4(0, -h, 0, 1)

    //add all vertices
    var vertices = <number[][]>verticesCircle1.slice()
    vertices.push(...verticesCircle2, vertexCenter1, vertexCenter2)

    //indexes
    var currentIndex = -1
    vertices.forEach(vertex => {
        (<any>vertex).index = ++currentIndex
    })

    //generate triangles
    var triangles: number[][] = []

    var addQuad = function (v1: number, v2: number, v3: number, v4: number)
    {
        triangles.push(vec3(v1, v2, v3))
        triangles.push(vec3(v2, v4, v3))
    }

    for (var i = 0; i < verticesCircle1.length - 1; ++i)
    {
        triangles.push(vec3(verticesCircle1[i].index, verticesCircle1[i + 1].index, vertexCenter1.index))
        triangles.push(vec3(verticesCircle2[i].index, verticesCircle2[i + 1].index, vertexCenter2.index))
        //
        addQuad(verticesCircle1[i].index, verticesCircle1[i + 1].index, verticesCircle2[i].index, verticesCircle2[i + 1].index)
    }
    triangles.push(vec3(verticesCircle1[verticesCircle1.length - 1].index, verticesCircle1[0].index, vertexCenter1.index))
    triangles.push(vec3(verticesCircle2[verticesCircle2.length - 1].index, verticesCircle2[0].index, vertexCenter2.index))
    //
    addQuad(verticesCircle1[verticesCircle1.length - 1].index, verticesCircle1[0].index,
        verticesCircle2[verticesCircle2.length - 1].index, verticesCircle2[0].index)


    var result = {
        triangles: triangles,
        vertices: vertices,
    }

    return result 
}


function CreateCone(size: number)
{
    var r = 0.7 * size
    var h = 0.9 * size
    var segmentCount = 20

    var verticesCircle: any[] = []

    var deltaTheta = 360 / segmentCount  //zx plane
    for (var theta = 0; theta < 360; theta += deltaTheta)
    {
        var theta_ = radians(theta)
        var y = 0
        var x = r * Math.cos(theta_)
        var z = r * Math.sin(theta_)
        verticesCircle.push(vec4(x, y, z, 1))
    }

    var vertexTop : any = vec4(0, h, 0, 1)
    var vertexCircleCenter : any = vec4(0, 0, 0, 1)

    //add all vertices
    var vertices = <number[][]>verticesCircle.slice()
    vertices.push(vertexTop, vertexCircleCenter)

    //indexes
    var currentIndex = -1
    vertices.forEach(vertex => {
        (<any>vertex).index = ++currentIndex
    })

    //generate triangles
    var triangles: number[][] = []
    for (var i = 0; i < verticesCircle.length - 1; ++i)
    {
        triangles.push(vec3(verticesCircle[i].index, verticesCircle[i + 1].index, vertexTop.index))
        triangles.push(vec3(verticesCircle[i].index, verticesCircle[i + 1].index, vertexCircleCenter.index))
    }
    triangles.push(vec3(verticesCircle[verticesCircle.length - 1].index, verticesCircle[0].index, vertexTop.index))
    triangles.push(vec3(verticesCircle[verticesCircle.length - 1].index, verticesCircle[0].index, vertexCircleCenter.index))

    var result = {
        triangles: triangles,
        vertices: vertices,
    }

    return result  
}


function CreateSphere(size : number)
{
    var r = 0.9 * size
    var segmentCount = 20

    ///
    var rings : number[][][] = []

    var deltaPhi = 180 / segmentCount //xy plane - outer ring
    var deltaTheta = 360 / segmentCount  //yz plane
    for (var phi = -90 + deltaPhi; phi <= 90 - deltaPhi; phi += deltaPhi)
    {
        var lastRing : number[][] = []
        rings.push(lastRing)

        for (var theta = 0; theta < 360; theta += deltaTheta)
        {
            var phi_ = radians(phi)
            var theta_ = radians(theta)

            var x = r * Math.sin(phi_)
            var y = r * Math.cos(phi_) * Math.cos(theta_)
            var z = r * Math.cos(phi_) * Math.sin(theta_)
            lastRing.push(vec4(x, y, z, 1))
        }
    }

    var poleLeft = <any>vec4(-r, 0, 0, 1);
    var poleRight = <any>vec4(r, 0, 0, 1)

    //collect vertices
    var vertices : number[][] = []
    vertices.push(poleLeft)
    rings.forEach(ring =>
    {
        ring.forEach(vertex =>
        {
            vertices.push(vertex)
        })
    })
    vertices.push(poleRight)

    //indexes
    var currentIndex = -1
    vertices.forEach(vertex =>
    {
        (<any>vertex).index = ++currentIndex
    })

    //---------generate triangles---------
    var triangles : number[][] = []
    //connect poles 
    for (var i = 0; i < rings[0].length - 1; ++i)
    {
        var ringZero: any[] = rings[0]
        var ringLast: any[] = rings[rings.length - 1]
        triangles.push(vec3(poleLeft.index, ringZero[i].index, ringZero[i + 1].index))
        triangles.push(vec3(poleRight.index, ringLast[i].index, ringLast[i + 1].index))
    }

    triangles.push(vec3(poleLeft.index, ringZero[ringZero.length - 1].index, ringZero[0].index))
    triangles.push(vec3(poleRight.index, ringLast[ringLast.length - 1].index, ringLast[0].index))

    var addQuad = function (v1: number, v2: number, v3: number, v4: number)
    {
        triangles.push(vec3(v1, v2, v3))
        triangles.push(vec3(v2, v4, v3))
    }
    
    //connect rings
    for (var i = 0; i < rings.length - 1; ++i)
    {
        var ring1 : any = rings[i]
        var ring2 : any = rings[i + 1]

        for (var j = 0; j < ring1.length - 1; ++j)
        {
            addQuad(ring1[j].index, ring1[j + 1].index, ring2[j].index, ring2[j + 1].index)
        }

        addQuad(ring1[ring1.length - 1].index, ring1[0].index, ring2[ring1.length - 1].index, ring2[0].index)
    }

    var result = {
        triangles : triangles,
        vertices: vertices,
    }

    return result    
}