class FileLoader {
    private rowIndex = 0;
    private charIndex = 0;
    private lines: string[];

    loadFile(content: string, zoom: number): Object2D {
        var find = "\t";
        var re = new RegExp(find, "g");
        content = content.replace(re, " ");
        this.lines = content.split("\n");

        this.find("MATERIAL_COUNT");
        var numMaterials = this.readNextInt();
        var materials = new Array<Material>();
        for (var i = 0; i < numMaterials; i++) {
            this.find("MATERIAL_NAME");
            var id = this.readNext();
            this.find("MATERIAL_DIFFUSE");
            var r = this.readNextFloat() * 255;
            var g = this.readNextFloat() * 255;
            var b = this.readNextFloat() * 255;
            var material = new Material(r, g, b, id);
            materials.push(material);
        }

        var meshes = new Array<Mesh>();
        while (this.find("GEOMOBJECT")) {
            this.find("NODE_NAME");
            var meshID = this.readNext();
            this.find("MESH_NUMVERTEX");
            var numVertex = this.readNextInt();
            this.find("MESH_NUMFACES");
            var numFaces = this.readNextInt();
            this.find("MESH_VERTEX_LIST");
            var vertices = new Array<Vertex>(numVertex);
            var x: number;
            var y: number;
            var z: number;
            for (var j = 0; j < numVertex; j++) {
                this.nextLine();
                this.find("MESH_VERTEX");
                this.readNext();
                x = this.readNextFloat() * zoom;
                y = this.readNextFloat() * zoom;
                z = this.readNextFloat() * zoom;
                vertices[j] = new Vertex(x, y, z);
            }

            this.find("MESH_FACE_LIST");
            var faces = new Array<Face>(numFaces);
            for (var k = 0; k < numFaces; k++) {
                this.nextLine();
                this.find("MESH_FACE");
                this.readNext(); //skip
                this.readNext(); //skip

                var a = this.readNextInt();
                this.readNext(); //skip
                var b = this.readNextInt();
                this.readNext(); //skip
                var c = this.readNextInt();

                this.find("AB:");
                var ab = this.readNextInt();
                this.find("BC:");
                var bc = this.readNextInt();
                this.find("CA:");
                var ca = this.readNextInt();

                faces[k] = new Face(a, b, c, ab > 0, bc > 0, ca > 0);
            }
            var vertexNormals = new Array<Vertex>(numVertex);
            this.find("MESH_NORMALS");
            for (var l = 0; l < numFaces; l++) {
                this.find("*MESH_FACENORMAL");
                this.find("*MESH_FACENORMAL");
                this.find("MESH_VERTEXNORMAL");
                j = this.readNextInt();
                x = this.readNextFloat();
                y = this.readNextFloat();
                z = this.readNextFloat();
                vertexNormals[j] = new Vertex(x, y, z);
                this.nextLine();
                this.find("MESH_VERTEXNORMAL");
                j = this.readNextInt();
                x = this.readNextFloat();
                y = this.readNextFloat();
                z = this.readNextFloat();
                vertexNormals[j] = new Vertex(x, y, z);
                this.nextLine();
                this.find("MESH_VERTEXNORMAL");
                j = this.readNextInt();
                x = this.readNextFloat();
                y = this.readNextFloat();
                z = this.readNextFloat();
                vertexNormals[j] = new Vertex(x, y, z);
            }
            this.find("MATERIAL_REF");
            var materialId = this.readNextInt();
            var polygons = new Array<Polygon>(numFaces);
            var outlines = new Array<Outline>(numFaces);
            for (var m = 0; m < numFaces; m++) {
                polygons[m] = new Polygon(faces[m].a, faces[m].b, faces[m].c);
                outlines[m] = new Outline(faces[m].ab, faces[m].bc, faces[m].ca);
            }

            var mesh = new Mesh(meshID, r, g, b, polygons, vertices, vertexNormals, outlines);
            meshes.push(mesh);
        }

        var res = new Object2D(meshes);
        return res;
    }

    nextLine() {
        this.rowIndex ++;
    }

    find(text: string): boolean {
        this.charIndex = 0;
        while (this.lines[this.rowIndex].indexOf(text) < 0) {
            this.rowIndex++;
            if (this.rowIndex >= this.lines.length)
                return false;
        }
        this.charIndex = this.lines[this.rowIndex].indexOf(text) + text.length;
        return true;
    }

    readNextFloat() {
        return parseFloat(this.readNext());
    }

    readNextInt() {
        return parseInt(this.readNext());
    }

    readNext() {
        while (this.lines[this.rowIndex].substr(this.charIndex, 1) === " ") {
            this.charIndex ++;
        }
        var start = this.charIndex;
        while (this.charIndex < this.lines[this.rowIndex].length && this.lines[this.rowIndex].substr(this.charIndex, 1) !== " ") {
            this.charIndex++;
        }
        var length = this.charIndex - start;
        return this.lines[this.rowIndex].substr(start, length);
    }
}