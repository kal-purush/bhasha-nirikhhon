

namespace GameOne.Source.Entities
{
    using System.Windows;

    using Renderer;

    class Projectile : Model
    {
        #region Fields

        private Vector velocity;

        #endregion Fields

        //===================================================================

        #region Constructors

        public Projectile(double x, double y, double direction, double radius, Spritesheet sprite, Character source, Vector velocity)
            : base(x, y, direction, radius, sprite)
        {
            this.Source = source;
            this.velocity = velocity;
        }

        #endregion Constructors

        //===================================================================

        #region Properties

        public Character Source { get; }

        #endregion Properties

        //===================================================================

        #region Methods

        public override void Update(double time)
        {
            if (Alive)
            {
                this.Position += this.velocity * time;
            }
        }

        public override void Render()
        {
            throw new System.NotImplementedException();
        }

        #endregion Methods
    }
}