using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class UvCalculator
{
    private enum FacingDirection { Up, Forward, Right };

    public static Vector2[] CalculateUVs(Vector3[] vertices, float scale, bool clipToBounds = false)
    {
        Vector2[] uvs = new Vector2[vertices.Length];
        Vector3 bounds = new Vector3();
        if (clipToBounds)
        {
            Vector3 startLocation = new Vector3(Mathf.Infinity, Mathf.Infinity, Mathf.Infinity);
            Vector3 endLocation = new Vector3(Mathf.NegativeInfinity, Mathf.NegativeInfinity, Mathf.NegativeInfinity);
            for (int i = 0; i < uvs.Length; i++)
            {
                RecalculateBounds(vertices[i], ref startLocation, ref endLocation);
            }
            bounds = endLocation - startLocation;
        }
        for (int i = 0; i < uvs.Length; i += 3)
        {
            int i0 = i;
            int i1 = i + 1;
            int i2 = i + 2;

            //In the case that the vertices are not a multiple of 3, loop back around on the final vertice. This script assumes the procedural mesh is generated loopwise, which makes this possible.
            if (i == uvs.Length - 1)
            {
                i1 = 0;
                i2 = 1;
            }
            if (i == uvs.Length - 2)
            {
                i2 = 0;
            }

            Vector3 v0 = vertices[i0];
            Vector3 v1 = vertices[i1];
            Vector3 v2 = vertices[i2];

            Vector3 side1 = v1 - v0;
            Vector3 side2 = v2 - v0;

            Vector3 direction = Vector3.Cross(side1, side2);
            FacingDirection normalDirection = CalculateDirection(direction);

            // Simply select which local position to use on the face depending on the normal direction of the face.
            switch (normalDirection)
            {
                case FacingDirection.Forward:
                    uvs[i0] = ScaleUV(v0.x, v0.y, scale, clipToBounds, bounds.x, bounds.y);
                    uvs[i1] = ScaleUV(v1.x, v1.y, scale, clipToBounds, bounds.x, bounds.y);
                    uvs[i2] = ScaleUV(v2.x, v2.y, scale, clipToBounds, bounds.x, bounds.y);
                    break;
                case FacingDirection.Up:
                    uvs[i0] = ScaleUV(v0.x, v0.z, scale, clipToBounds, bounds.x, bounds.z);
                    uvs[i1] = ScaleUV(v1.x, v1.z, scale, clipToBounds, bounds.x, bounds.z);
                    uvs[i2] = ScaleUV(v2.x, v2.z, scale, clipToBounds, bounds.x, bounds.z);
                    break;
                case FacingDirection.Right:
                    uvs[i0] = ScaleUV(v0.y, v0.z, scale, clipToBounds, bounds.y, bounds.z);
                    uvs[i1] = ScaleUV(v1.y, v1.z, scale, clipToBounds, bounds.y, bounds.z);
                    uvs[i2] = ScaleUV(v2.y, v2.z, scale, clipToBounds, bounds.y, bounds.z);
                    break;
            }
        }
        return uvs;
    }

    // Dot product based algorithm which calculates in which direction the face in question is facing, to allow for a simple triplanar map to be thrown over it.
    private static FacingDirection CalculateDirection(Vector3 v)
    {
        FacingDirection normalDirection = FacingDirection.Up;
        float maxDot = 0;

        // If NOT right, then left, is the most efficient way to run this algorithm, because we get to discard a lot of calculations in the case the correct direction is indeed right.
        if (!DotCalculator(v, Vector3.right, FacingDirection.Right, ref maxDot, ref normalDirection))
            DotCalculator(v, Vector3.left, FacingDirection.Right, ref maxDot, ref normalDirection);

        if (!DotCalculator(v, Vector3.forward, FacingDirection.Forward, ref maxDot, ref normalDirection))
            DotCalculator(v, Vector3.back, FacingDirection.Forward, ref maxDot, ref normalDirection);

        if (!DotCalculator(v, Vector3.up, FacingDirection.Up, ref maxDot, ref normalDirection))
            DotCalculator(v, Vector3.down, FacingDirection.Up, ref maxDot, ref normalDirection);

        return normalDirection;
    }

    // Does a dor product calculation, then updates the facing direction for this face if this is the highest dot product we have found so far.
    private static bool DotCalculator(Vector3 v, Vector3 dir, FacingDirection p, ref float maxDot, ref FacingDirection normalDirection)
    {
        float result = Vector3.Dot(v, dir);
        if (result > maxDot)
        {
            normalDirection = p;
            maxDot = result;
            return true;
        }
        return false;
    }

    private static Vector2 ScaleUV(float uv1, float uv2, float scale, bool clipToBounds = false, float boundsX = 1, float boundsY = 1)
    {
        if (clipToBounds)
        {
            return new Vector2(uv1 / ((1/scale) * boundsX), uv2 / ((1 / scale) * boundsY));
        }
        return new Vector2(uv1 / scale, uv2 / scale);
    }
    private static void RecalculateBounds(Vector3 vertex, ref Vector3 startLocation, ref Vector3 endLocation)
    {
        if (vertex.x < startLocation.x) startLocation.x = vertex.x;
        if (vertex.y < startLocation.y) startLocation.y = vertex.y;
        if (vertex.z < startLocation.z) startLocation.z = vertex.z;
        if (vertex.x > endLocation.x) endLocation.x = vertex.x;
        if (vertex.y > endLocation.y) endLocation.y = vertex.y;
        if (vertex.z > endLocation.z) endLocation.z = vertex.z;
    }
}