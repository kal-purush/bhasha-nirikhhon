using Godot;
using System;
using System.Collections.Generic;

/* A PaintBall object cannot be "directly" rendered.
* Instead, it contains information needed to render a paintball. It needs to be passed into a PaintBallGroup object in order to be rendered.
*/

public partial class PaintBall
{
    private Vector3 coordinations = new Vector3(1.0, 0.0, 0.0);
    private float size = 1.0;
    private float colorIndex = 0.0;
    
    public Vector3 Coordinations = () => coordinations;
    public float Size = () => size;
    public float ColorIndex = () => colorIndex;
    
    public PaintBall()
    {
    
    }
    
    public PaintBall(Vector3 _coords, float _size, float _colorIndex)
    {
        coordinations = _coords;
        size = _size / 100.0;
        colorIndex = _colorIndex;
    }
}