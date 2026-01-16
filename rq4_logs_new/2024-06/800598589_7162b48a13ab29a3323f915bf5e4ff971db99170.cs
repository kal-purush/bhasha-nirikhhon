using Godot;

public partial class Camera : Camera2D
{
    [Export(PropertyHint.Range, "100,1000,10,or_greater")]
    public float MoveSpeed { get; set; } = 300.0f; // in px/sec
    
    [Export(PropertyHint.Range, "0.1,2.0,0.1,or_greater")]
    public float ZoomSpeed { get; set; } = 1.0f; // per sec
    
    [Export(PropertyHint.Range, "1.0,5.0,0.1,or_greater")]
    public float MaxZoom { get; set; } = 3.0f;
    
    [Export(PropertyHint.Range, "0.1,1.0,0.1")]
    public float MinZoom { get; set; } = 0.3f;
    
    private Vector2 moveDirection = Vector2.Zero;
    private bool zoomingIn = false;
    private bool zoomingOut = false;
    
    public override void _Input(InputEvent @event)
    {
        if (@event.IsActionPressed("move.camera.up"))
        {
            this.moveDirection += Vector2.Up;
        }
        if (@event.IsActionPressed("move.camera.right"))
        {
            this.moveDirection += Vector2.Right;
        }
        if (@event.IsActionPressed("move.camera.down"))
        {
            this.moveDirection += Vector2.Down;
        }
        if (@event.IsActionPressed("move.camera.left"))
        {
            this.moveDirection += Vector2.Left;
        }
        if (@event.IsActionPressed("zoom.camera.in"))
        {
            this.zoomingIn = true;
        }
        if (@event.IsActionPressed("zoom.camera.out"))
        {
            this.zoomingOut = true;
        }
        
        if (@event.IsActionReleased("move.camera.up"))
        {
            this.moveDirection -= Vector2.Up;
        }
        if (@event.IsActionReleased("move.camera.right"))
        {
            this.moveDirection -= Vector2.Right;
        }
        if (@event.IsActionReleased("move.camera.down"))
        {
            this.moveDirection -= Vector2.Down;
        }
        if (@event.IsActionReleased("move.camera.left"))
        {
            this.moveDirection -= Vector2.Left;
        }
        if (@event.IsActionReleased("zoom.camera.in"))
        {
            this.zoomingIn = false;
        }
        if (@event.IsActionReleased("zoom.camera.out"))
        {
            this.zoomingOut = false;
        }
    }

    public override void _PhysicsProcess(double delta)
    {
        this.UpdatePosition(delta);
        this.UpdateZoom(delta);
        GD.Print(this.Zoom);
    }

    private void UpdatePosition(double delta)
    {
        this.GlobalPosition += this.moveDirection * this.MoveSpeed * (float)delta;
    }

    private void UpdateZoom(double delta)
    {
        Vector2 zoomRatio = Vector2.Zero;
        if (this.zoomingIn) zoomRatio += new Vector2(this.ZoomSpeed, this.ZoomSpeed);
        if (this.zoomingOut) zoomRatio -= new Vector2(this.ZoomSpeed, this.ZoomSpeed);

        this.Zoom += zoomRatio * (float)delta;
        this.Zoom = this.Zoom.Clamp(new Vector2(this.MinZoom, this.MinZoom), new Vector2(this.MaxZoom, this.MaxZoom));
    }
}