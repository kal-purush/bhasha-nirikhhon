using Box2DSharp.Collision.Collider;
using Box2DSharp.Dynamics;
using Box2DSharp.Dynamics.Contacts;
using MonoGame.Extended.Entities;

namespace Platformer
{
    internal class EdgeShapeContactListener : IContactListener
    {
        private MonoGame.Extended.Entities.World _world;

        public EdgeShapeContactListener(MonoGame.Extended.Entities.World world)
        {
            _world = world;
        }

        public void BeginContact(Contact contact)
        {
        }

        public void EndContact(Contact contact)
        {
        }

        public void PostSolve(Contact contact, in ContactImpulse impulse)
        {
            Entity entityA = _world.GetEntity((int)contact.FixtureA.Body.UserData);
            Entity entityB = _world.GetEntity((int)contact.FixtureB.Body.UserData);
            
            System.Diagnostics.Debug.WriteLine($"          entity A: {entityA.Id}");
            System.Diagnostics.Debug.WriteLine($"           shape A: {contact.FixtureA.ShapeType}");
            System.Diagnostics.Debug.WriteLine($"          entity B: {entityB.Id}");
            System.Diagnostics.Debug.WriteLine($"           shape B: {contact.FixtureB.ShapeType}");

            System.Diagnostics.Debug.WriteLine($"  impulse normal 0: {impulse.NormalImpulses.Value0}");
            System.Diagnostics.Debug.WriteLine($"  impulse normal 1: {impulse.NormalImpulses.Value1}");
        }

        public void PreSolve(Contact contact, in Manifold oldManifold)
        {
        }
    }
}