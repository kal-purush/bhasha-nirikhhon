using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework;

using AIRogue.GameObjects;
using AIRogue.Agent.Brain.Constructs;

namespace AIRogue.Agent
{
    public class Agent
    {
        private Controller brain;
        private Vision eyes;
        private Actor body;
        private Memory memory;
        private World world;
        public bool IsCameraTarget { get; set; }
        public bool HasMoved { get; set; } = false;

        /// <summary>
        /// The position of the Agent relative to where it started.
        /// </summary>
        public Vector2 MemoryPosition {
            get
            {
                return memory.position;
            }
        }

        /// <summary>
        /// Setup the Agent.
        /// </summary>
        /// <param name="_brain"></param>
        /// <param name="_eyes"></param>
        /// <param name="_body"></param>
        /// <param name="_world"></param>
        public void Setup (Controller _brain, Vision _eyes, Actor _body, World _world)
        {
            this.brain = _brain;
            this.eyes = _eyes;
            this.body = _body;
            this.world = _world;
            this.memory = new Memory(this);
            this.memory.AddSet(eyes.CheckSurroundings());
        }

        // *** FOR THE BRAIN *** //
        /// <summary>
        /// Set next Action that the Agent will take.
        /// </summary>
        public void GetNextAction()
        {
            Action.Option action = this.brain.GetAction();
            this.body.SetAction(action);
        }

        /// <summary>
        /// Returns a ConstructLocation based on it's position relative to the Agent.
        /// </summary>
        /// <param name="relativePosition">Position of the Location relative to the Agent.</param>
        /// <returns>ConstructLocation</returns>
        public ConstructLocation RecallRelativeLocation(Vector2 relativePosition)
        {
            return this.memory.locationDict[relativePosition + MemoryPosition];
        }

        /// <summary>
        /// Returns a ConstructLocation based on it's address in the Agent's Memory.
        /// </summary>
        /// <param name="memoryPosition">The address of the ConstructLocation.</param>
        /// <returns>ConstructLocation</returns>
        public ConstructLocation RecallMemoryLocation(Vector2 memoryPosition)
        {
            return this.memory.locationDict[memoryPosition];
        }

        /// <summary>
        /// Update the score of a ConstructLocation in the Agent's Memory.
        /// </summary>
        /// <param name="locationPosition">Address of the ConstructLocation who's score will be changed.</param>
        /// <param name="newScore">The number that the score should be changed to.</param>
        public void UpdateLocationScore(Vector2 locationPosition, float newScore)
        {
            this.memory.locationDict[locationPosition].Score.IntrinsicScore = newScore;
        }

        /// <summary>
        /// Returns a list of all ConstructLocation addresses in the Agent's Memory.
        /// </summary>
        /// <returns>List of all ConstructLocation addresses in the Agent's Memory</returns>
        public List<Vector2> GetRememberedLocations()
        {
            return this.memory.locationDict.Keys.ToList<Vector2>();
        }

        // *** FOR THE BODY *** //
        /// <summary>
        /// To be called by Actor every time the Actor moves
        /// </summary>
        /// <param name="delta">The Vector2 by which the Actor moved</param>
        public void OnAgentMove(Vector2 delta)
        {
            this.memory.position += delta;
            this.HasMoved = true;
        }

        /// <summary>
        /// Update Agent memory after Agent has taken an Action.
        /// </summary>
        public void OnAgentAction()
        {
            this.memory.AddSet(eyes.CheckSurroundings());
            Engine.ObjectHandler.gameCamera.LerpToPosition(MemoryPosition * 64);
        }

        // *** FOR THE EYES *** //
        /// <summary>
        /// Request a report on a Location within the agents perception.
        /// </summary>
        /// <param name="positionRequest">Requested location relative to actor</param>
        /// <returns>LocationConstruct</returns>
        public ConstructLocation CreateLocationReport(Vector2 relativePosition)
        {
            if (relativePosition.Length() > eyes.VisionRange) {
                return new ConstructLocation();
            }

            Vector2 checkPosition = body.Position + relativePosition;

            Location requestedLocation = world.GetLocation(checkPosition);

            if (requestedLocation == null)
            {
                return new ConstructLocation();
            } else
            {
                return new ConstructLocation(requestedLocation);
            }
        }

        /// <summary>
        /// Check if a Location is currently visible. 
        /// </summary>
        /// <param name="checkPosition"></param>
        /// <returns></returns>
        public bool IsVisible(Vector2 checkPosition)
        {
            return (checkPosition - MemoryPosition).Length() <= eyes.VisionRange;
        }

        /// <summary>
        /// Used to display what the agent knows on the screen.
        /// </summary>
        public void DisplayWorld()
        {
            Vector2 offset = MemoryPosition - body.Position;
            Color filter;
            ConstructLocation location;
            SpriteFont font = Engine.ObjectHandler.GetFont();
               
            foreach (Vector2 key in memory.locationDict.Keys)
            {
                location = memory.locationDict[key];
                filter = IsVisible(key) ? Color.White : Color.Gray;
                location.Drawable.Draw(key, filter);
                location.Item.Drawable.Draw(key, filter);
                Engine.Drawable.spriteBatch.DrawString(font, location.Score.Score.ToString(), key * 65, Color.Black);
            }
            body.Drawable.Draw(MemoryPosition);
        }
    }
}