using VectorAndPoint.ValTypes;

namespace _2022.Days;

public class Day18 : Day
{
    private readonly Dictionary<Point3D, Point3D> _exposedFaceMidpoints = new();
    
    public Day18() : base(18)
    {
    }

    protected override void ProcessInputLine(string line)
    {
        var coords = line.Split(',');

        if (coords.Length is not 3)
            throw new ArgumentException($"Expected {line} to be a 3-part co-ordinate");

        if (int.TryParse(coords[0], out var x) is false)
            throw new ArgumentException($"Expected {coords[0]} to be an integer");
        
        if (int.TryParse(coords[1], out var y) is false)
            throw new ArgumentException($"Expected {coords[1]} to be an integer");
        
        if (int.TryParse(coords[2], out var z) is false)
            throw new ArgumentException($"Expected {coords[2]} to be an integer");

        var cubeStartPoint = new Point3D(x, y, z);

        var faces = GetFaces(cubeStartPoint);

        var cubeMidPoint = new Point3D(x + 0.5, y + 0.5, z + 0.5);

        foreach (var face in faces)
        {
            if (this._exposedFaceMidpoints.Remove(face) is false)
            {
                // Remove if already there, add if not
                this._exposedFaceMidpoints.Add(face, cubeMidPoint);
            }
        }
    }

    private static IEnumerable<Point3D> GetFaces(Point3D cubeStart)
    {
        var oppositeCorner = new Point3D(cubeStart.X + 1, cubeStart.Y + 1, cubeStart.Z + 1);

        var faces = new HashSet<Point3D>
        {
            // The 3 faces touching the cubeStart corner
            new(cubeStart.X + 0.5, cubeStart.Y + 0.5, cubeStart.Z),
            new(cubeStart.X, cubeStart.Y + 0.5, cubeStart.Z + 0.5),
            new(cubeStart.X + 0.5, cubeStart.Y, cubeStart.Z + 0.5),
            // The 3 faces touching the opposite corner
            new(oppositeCorner.X - 0.5, oppositeCorner.Y - 0.5, oppositeCorner.Z),
            new(oppositeCorner.X, oppositeCorner.Y - 0.5, oppositeCorner.Z - 0.5),
            new(oppositeCorner.X - 0.5, oppositeCorner.Y, oppositeCorner.Z - 0.5)
        };

        return faces;
    }

    public override void SolvePart1()
    {
        this.Part1Solution = this._exposedFaceMidpoints.Count.ToString();
    }

    public override void SolvePart2()
    {
        var surfaces = new List<HashSet<Point3D>>();
        
        var consideredFaces = new HashSet<Point3D>();
        
        var currentSurface = new HashSet<Point3D>();

        foreach (var exposedFace in this._exposedFaceMidpoints)
        {
            if (consideredFaces.Contains(exposedFace.Key))
                continue;
            
            var faceQueue = new Queue<Point3D>();
            faceQueue.Enqueue(exposedFace.Key);

            while (faceQueue.TryDequeue(out var face))
            {
                currentSurface.Add(face);
                consideredFaces.Add(face);

                var adjacentFaces = this.GetAdjacentFaces(face);

                foreach (var adjacentFace in adjacentFaces)
                {
                    if (currentSurface.Contains(adjacentFace) is false)
                    {
                        faceQueue.Enqueue(adjacentFace);
                    }
                }
            }

            surfaces.Add(currentSurface);
            currentSurface = new HashSet<Point3D>();
        }

        this.Part2Solution = surfaces.Select(s => s.Count).Max().ToString();
    }

    private IEnumerable<Point3D> GetAdjacentFaces(Point3D face)
    {
        var adjacentFaces = new HashSet<Point3D>();

        var cubeCenter = this._exposedFaceMidpoints[face];
        var velToCenter = GetVelocity(face, cubeCenter);
        
        // Check x +/- 0.5
        if (velToCenter.X is 0)
        {
            // Positive x direction
            var faceToCheck = new Point3D(face.X + 0.5, face.Y - velToCenter.Y, face.Z - velToCenter.Z);

            if (this._exposedFaceMidpoints.ContainsKey(faceToCheck))
            {
                // Extruded face
                adjacentFaces.Add(faceToCheck);
            }
            else
            {
                faceToCheck = new Point3D(face.X + 1, face.Y, face.Z);

                adjacentFaces.Add(this._exposedFaceMidpoints.ContainsKey(faceToCheck)
                    ? faceToCheck // Face in same plane
                    : new Point3D(face.X + 0.5, face.Y + velToCenter.Y, face.Z + velToCenter.Z)); // Face across edge of this cube
            }
            
            // Negative x direction
            faceToCheck = new Point3D(face.X - 0.5, face.Y - velToCenter.Y, face.Z - velToCenter.Z);

            if (this._exposedFaceMidpoints.ContainsKey(faceToCheck))
            {
                // Extruded face
                adjacentFaces.Add(faceToCheck);
            }
            else
            {
                faceToCheck = new Point3D(face.X - 1, face.Y, face.Z);

                adjacentFaces.Add(this._exposedFaceMidpoints.ContainsKey(faceToCheck)
                    ? faceToCheck // Face in same plane
                    : new Point3D(face.X - 0.5, face.Y + velToCenter.Y, face.Z + velToCenter.Z)); // Face across edge of this cube
            }
        }
        
        if (velToCenter.Y is 0)
        {
            // Positive y direction
            var faceToCheck = new Point3D(face.X - velToCenter.X, face.Y + 0.5, face.Z - velToCenter.Z);

            if (this._exposedFaceMidpoints.ContainsKey(faceToCheck))
            {
                // Extruded face
                adjacentFaces.Add(faceToCheck);
            }
            else
            {
                faceToCheck = new Point3D(face.X, face.Y + 1, face.Z);

                adjacentFaces.Add(this._exposedFaceMidpoints.ContainsKey(faceToCheck)
                    ? faceToCheck // Face in same plane
                    : new Point3D(face.X + velToCenter.X, face.Y + 0.5, face.Z + velToCenter.Z)); // Face across edge of this cube
            }
            
            // Negative y direction
            faceToCheck = new Point3D(face.X - velToCenter.X, face.Y - 0.5, face.Z - velToCenter.Z);

            if (this._exposedFaceMidpoints.ContainsKey(faceToCheck))
            {
                // Extruded face
                adjacentFaces.Add(faceToCheck);
            }
            else
            {
                faceToCheck = new Point3D(face.X, face.Y - 1, face.Z);

                adjacentFaces.Add(this._exposedFaceMidpoints.ContainsKey(faceToCheck)
                    ? faceToCheck // Face in same plane
                    : new Point3D(face.X + velToCenter.X, face.Y - 0.5, face.Z + velToCenter.Z)); // Face across edge of this cube
            }
        }
        
        if (velToCenter.Z is 0)
        {
            // Positive x direction
            var faceToCheck = new Point3D(face.X - velToCenter.X, face.Y - velToCenter.Y, face.Z + 0.5);

            if (this._exposedFaceMidpoints.ContainsKey(faceToCheck))
            {
                // Extruded face
                adjacentFaces.Add(faceToCheck);
            }
            else
            {
                faceToCheck = new Point3D(face.X, face.Y, face.Z + 1);

                adjacentFaces.Add(this._exposedFaceMidpoints.ContainsKey(faceToCheck)
                    ? faceToCheck // Face in same plane
                    : new Point3D(face.X + velToCenter.X, face.Y + velToCenter.Y, face.Z + 0.5)); // Face across edge of this cube
            }
            
            // Negative x direction
            faceToCheck = new Point3D(face.X - velToCenter.X, face.Y - velToCenter.Y, face.Z - 0.5);

            if (this._exposedFaceMidpoints.ContainsKey(faceToCheck))
            {
                // Extruded face
                adjacentFaces.Add(faceToCheck);
            }
            else
            {
                faceToCheck = new Point3D(face.X, face.Y, face.Z - 1);

                adjacentFaces.Add(this._exposedFaceMidpoints.ContainsKey(faceToCheck)
                    ? faceToCheck // Face in same plane
                    : new Point3D(face.X + velToCenter.X, face.Y + velToCenter.Y, face.Z - 0.5)); // Face across edge of this cube
            }
        }

        return adjacentFaces;
    }

    private static Point3D GetVelocity(Point3D p1, Point3D p2)
    {
        return new Point3D(p2.X - p1.X, p2.Y - p1.Y, p2.Z - p1.Z);
    }
}