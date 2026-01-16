using Microsoft.Xna.Framework;
using MononokeEngine.ECS;
using MononokeEngine.Physics;
using MononokeEngine.Physics.Colliders;
using MononokeEngine.Scenes;
using NUnit.Framework;

namespace MononokeEngine.Tests.Physics
{
    
    [TestFixture]
    public class SimpleCollisionTest
    {
        private Scene _scene;

        private int _onEnter1, _onStay1, _onExit1;
        private int _onEnter2, _onStay2, _onExit2;
        
        
        

        [Test]
        public void Test()
        {
            _scene = new Scene();
            
            BoxCollider bc1 = new BoxCollider(32, 32);
            bc1.OnCollisionEnter += other => _onEnter1++;
            bc1.OnCollisionStay += other => _onStay1++;
            bc1.OnCollisionExit += other => _onExit1++;
            
            BoxCollider bc2 = new BoxCollider(32, 32);
            bc2.OnCollisionEnter += other => _onEnter2++;
            bc2.OnCollisionStay += other => _onStay2++;
            bc2.OnCollisionExit += other => _onExit2++;
            
            Entity e1 = new Entity();
            e1.Bind(bc1);
            e1.Position = new Vector2(-16, 0);
            
            Entity e2 = new Entity(); 
            e2.Bind(bc2);
            e2.Position = new Vector2(16, 0);
            
            _scene.Add(e1);
            _scene.Add(e2);
            
            Tick();

            AssertOnEnter(0);
            AssertOnStay(0);
            AssertOnExit(0);
            
            e1.Position += Vector2.One;
            Tick();
            
            AssertOnEnter(1);
            AssertOnStay(0);
            AssertOnExit(0);
            
            Tick();
            
            AssertOnEnter(1);
            AssertOnStay(1);
            AssertOnExit(0);

            Tick();
            
            AssertOnEnter(1);
            AssertOnStay(2);
            AssertOnExit(0);
        }

        private void Tick()
        {
            _scene.BeforeUpdate();
            _scene.Update();
            _scene.AfterUpdate();
        }

        private void AssertOnEnter(int value)
        {
            Assert.AreEqual(value, _onEnter1);
            Assert.AreEqual(value, _onEnter2);
        }
        
        private void AssertOnStay(int value)
        {
            Assert.AreEqual(value, _onStay1);
            Assert.AreEqual(value, _onStay2);
        }
        
        private void AssertOnExit(int value)
        {
            Assert.AreEqual(value, _onExit1);
            Assert.AreEqual(value, _onExit2);
        }
        
    }
}