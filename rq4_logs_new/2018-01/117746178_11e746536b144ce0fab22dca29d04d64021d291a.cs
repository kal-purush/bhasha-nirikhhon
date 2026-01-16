using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using System;

class Animation : Image
{
    #region Variables
    protected TimeSpan elapsedTime;
    protected TimeSpan frameTime;
    protected Vector2 sourceOrigin;
    protected int rows;
    protected int currentRow;
    protected int columns;
    protected int currentColumn;
    protected bool looping;
    protected bool loopComplete;
    #endregion

    #region Constructors
    public Animation(Texture2D texture, SpriteBatch spriteBatch, Vector2 sourceOrigin, Vector2 sourceDimensions, Vector2 drawPosition, Vector2 drawDimensions, Color color, TimeSpan frameTime, int rows, int columns, float rotation = 0f, bool visible = true, bool active = true, bool looping = true)
        : base(texture, spriteBatch, sourceOrigin, sourceDimensions, drawPosition, drawDimensions, color, rotation, visible, active)
    {
        this.sourceOrigin = sourceOrigin;
        this.rows = rows;
        this.columns = columns;
        this.looping = looping;
        this.frameTime = frameTime;
        if(this.frameTime == null) frameTime = Configuration.DefaultFrameTime;

        currentRow = 0;
        currentColumn = 0;
    }
    #endregion

    #region Public Methods
    public override void Update(GameTime gameTime)
    {
        if(!active || (!looping && loopComplete)) return;

        elapsedTime += gameTime.ElapsedGameTime;
        if (elapsedTime >= frameTime)
        {
            currentColumn++;
            if (currentColumn >= columns)
            {
                currentColumn = 0;
                currentRow++;
                if (currentRow >= rows)
                {
                    currentRow = 0;
                    loopComplete = true;
                }
            }
        }

        CreateSourceRectangle();
    }
    #endregion

    #region Protected Methods
    protected override void CreateSourceRectangle()
    {
        sourcePosition = new Vector2(sourceOrigin.X + (currentColumn * sourceDimensions.X)
            , sourceOrigin.Y + (currentRow * sourceDimensions.Y));

        base.CreateSourceRectangle();
    }
    #endregion
}