namespace Mentula.GuiItems.Core
{
    using Microsoft.Xna.Framework;

    /// <summary> Base class for all drawable Mentula game components. </summary>
    /// <typeparam name="TGame"> The specified game class. </typeparam>
#if !DEBUG
    [System.Diagnostics.DebuggerStepThrough]
#endif
    public abstract class DrawableMentulaGameComponent<TGame> : DrawableGameComponent
        where TGame : Game
    {
        /// <summary> Gets the Game associated with this GameComponent. </summary>
        new public TGame Game { get { return (TGame)base.Game; } }

        /// <summary> Initializes a new instance of the <see cref="DrawableMentulaGameComponent{TGame}"/> class. </summary>
        /// <param name="game"> The game associated with this component. </param>
        protected DrawableMentulaGameComponent(TGame game)
            : base(game)
        { }
    }
}