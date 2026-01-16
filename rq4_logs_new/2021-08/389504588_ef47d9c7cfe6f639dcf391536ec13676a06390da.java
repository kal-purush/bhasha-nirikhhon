package com.bramerlabs.engine3D.objects;

import com.bramerlabs.engine3D.graphics.mesh.ColorMesh;
import com.bramerlabs.engine3D.graphics.vertex.ColorVertex;
import com.bramerlabs.engine3D.math.vector.Vector3f;
import com.bramerlabs.engine3D.math.vector.Vector4f;

import java.io.*;
import java.util.ArrayList;
import java.util.Vector;

public class ObjectLoader {

    public static RenderObject parse(String pathToFile) {
        Vector3f position = new Vector3f(0, 0, 0);
        Vector3f rotation = new Vector3f(0, 0, 0);
        Vector3f scale = new Vector3f(1, 1, 1);

        Vector<Float> vertices = new Vector<>();
        Vector<Float> normals = new Vector<>();
        ArrayList<String> faces = new ArrayList<>();

        try {
            BufferedReader objectReader = new BufferedReader(new FileReader(pathToFile));
            String line;
                while ((line = objectReader.readLine()) != null) {
                    String[] parts = line.split(" ");
                    switch (parts[0]) {
                        case "v":
                            vertices.add(Float.valueOf(parts[1]));
                            vertices.add(Float.valueOf(parts[2]));
                            vertices.add(Float.valueOf(parts[3]));
                            break;
                        case "vn":
                            normals.add(Float.valueOf(parts[1]));
                            normals.add(Float.valueOf(parts[2]));
                            normals.add(Float.valueOf(parts[3]));
                            break;
                        case "f":
                            // faces: vertex/texture/normal
                            faces.add(parts[1]);
                            faces.add(parts[2]);
                            faces.add(parts[3]);
                            break;
                    }
                }
        } catch (IOException e) {
            e.printStackTrace();
        }

        ArrayList<ColorVertex> colorVertices = new ArrayList<>();

        double x = 0, y = 0, z = 0, t = 0;

        for (String face : faces) {
            String[] parts = face.split("/");

            int index = 3 * (Integer.parseInt(parts[0]) - 1);
            Vector3f p = new Vector3f(vertices.get(index++), vertices.get(index++), vertices.get(index));
            x += p.x;
            y += p.y;
            z += p.z;
            t += 1;

            index = 3 * (Integer.parseInt(parts[2]) - 1);
            Vector3f n = new Vector3f(normals.get(index++), normals.get(index++), normals.get(index));

            colorVertices.add(new ColorVertex(p, n, new Vector4f(0.5f, 0.5f, 0.5f, 1)));
        }

        ColorVertex[] c = colorVertices.toArray(new ColorVertex[0]);
        int[] indices = new int[c.length];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = i;
        }

        return new RenderObject(new ColorMesh(c, indices), position, rotation, scale);

    }

}