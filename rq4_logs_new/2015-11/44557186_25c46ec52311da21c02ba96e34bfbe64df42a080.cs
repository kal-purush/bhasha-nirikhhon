/*
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework.Input;
using System.Threading;

namespace COSC438GameDesignNewREV
{
    public class PlayerCollision
    {
        private Game1 gameObj;
        private Physics physicsEngine;
        public PlayerCollision(Game1 gameObj)
        {
            this.gameObj = gameObj;
            this.physicsEngine = gameObj.PhysicsEngine;
        }
        public bool SweepCollisionVertical()
        {
            //Need to implement sweeping detection to fix collision graph offsets              
            int checkX = gameObj.currPlayerPositionFunc.X;
            int checkY = gameObj.currPlayerPositionFunc.Y + (int)physicsEngine.Velocity.Y;
            Rectangle checkPlayerNewState = new Rectangle(checkX, checkY - gameObj.ActivePlayer.Height + physicsEngine.YOffSet, 40, 40);
            if (CollisionDetection(checkPlayerNewState))
            {
                return true;
            }
            return false;
        }
        public bool SweepCollisionHorizontal()
        {
            //Need to implement sweeping detection to fix collision graph offsets              
            int checkX = gameObj.currPlayerPositionFunc.X + (int)physicsEngine.Velocity.X;
            int checkY = gameObj.currPlayerPositionFunc.Y;
            Rectangle checkPlayerNewState = new Rectangle(checkX, checkY - gameObj.ActivePlayer.Height + physicsEngine.YOffset, 40, 40);
            if (CollisionDetection(checkPlayerNewState))
            {
                return true;
            }
            return false;
        }
        //Standard game collision detection algorithm, citation at top of file.
        public bool CollisionDetection(Rectangle checkPlayerNewState)
        {
            //Collision for platform tiles require movement from top collisions
            foreach (PlatFormTile tile in gameObj.PhysicsEngine.GenMaps[physicsEngine.ACTIVELEVELFunc].PlatFormTile)
            {
                //Right side collison with offsets at maximum velocity
                if (checkPlayerNewState.Left >= tile.Box.Left && checkPlayerNewState.Left <= tile.Box.Right && checkPlayerNewState.Top <= tile.Box.Bottom - (gameObj.ActivePlayer.Height / 2 - 10) && checkPlayerNewState.Bottom >= tile.Box.Top + (gameObj.ActivePlayer.Width / 4))
                {
                    //  Console.WriteLine("Right Collision");    
                    gameObj.currPlayerPositionFunc.X = tile.Box.Right + 2;
                    return true;
                }
                //Left side collisions with offsets
                if (checkPlayerNewState.Right <= tile.Box.Right && checkPlayerNewState.Right >= tile.Box.Left && checkPlayerNewState.Top <= tile.Box.Bottom - (gameObj.ActivePlayer.Height / 2 - 10) && checkPlayerNewState.Bottom >= tile.Box.Top + (gameObj.ActivePlayer.Width / 4))
                {
                    // Console.WriteLine("Left Collision");
                    gameObj.currPlayerPositionFunc.X = tile.Box.Left - 2 - gameObj.activePlayerFunc.Width;
                    return true;
                }
                //Top
                if (checkPlayerNewState.Bottom >= tile.Box.Top && checkPlayerNewState.Bottom <= tile.Box.Top + (gameObj.ActivePlayer.Height / 2) + 1 && checkPlayerNewState.Right >= tile.Box.Left + (gameObj.ActivePlayer.Height / 10 - 1) && checkPlayerNewState.Left <= tile.Box.Right - (gameObj.ActivePlayer.Height / 10 - 1))
                {
                    // Console.WriteLine("Top Collision");
                    physicsEngine.Jumpstate = false;
                    physicsEngine.SetVelocity(-1, 0);
                    gameObj.currPlayerPositionFunc.Y = tile.Box.Top - 1;
                    return true;
                }
                //Bottom
                if (checkPlayerNewState.Top <= tile.Box.Bottom + gameObj.ActivePlayer.Height - physicsEngine.YOffSet - 2 && checkPlayerNewState.Top >= tile.Box.Bottom - 1 && checkPlayerNewState.Right >= tile.Box.Left + (gameObj.ActivePlayer.Height / 10 - 1) && checkPlayerNewState.Left <= tile.Box.Right - (gameObj.ActivePlayer.Height / 10 - 1))
                {
                    // Console.WriteLine("Bot Collision");                       
                    physicsEngine.SetVelocity(-1, 1);
                    gameObj.currPlayerPositionFunc.Y = tile.Box.Bottom + gameObj.ActivePlayer.Height + 2;
                    return true;
                }
            }
            foreach (ItemTile tile in gameObj.GenMaps[physicsEngine.ACTIVELEVELFunc].ItemTile)
            {
                //Right side collison with offsets at maximum velocity
                if (checkPlayerNewState.Left >= tile.Box.Left && checkPlayerNewState.Left <= tile.Box.Right && checkPlayerNewState.Top <= tile.Box.Bottom - (gameObj.ActivePlayer.Height / 2 - 10) && checkPlayerNewState.Bottom >= tile.Box.Top + (gameObj.ActivePlayer.Width / 4))
                {
                    gameObj.InventoryContainer[tile.ItemKey].Collected = true;
                    tile.Collected = true;
                }
                //Left side collisions with offsets
                if (checkPlayerNewState.Right <= tile.Box.Right && checkPlayerNewState.Right >= tile.Box.Left && checkPlayerNewState.Top <= tile.Box.Bottom - (gameObj.ActivePlayer.Height / 2 - 10) && checkPlayerNewState.Bottom >= tile.Box.Top + (gameObj.ActivePlayer.Width / 4))
                {
                    gameObj.InventoryContainer[tile.ItemKey].Collected = true;
                    tile.Collected = true;
                }
                //Top
                if (checkPlayerNewState.Bottom >= tile.Box.Top && checkPlayerNewState.Bottom <= tile.Box.Top + (gameObj.ActivePlayer.Height / 2) + 1 && checkPlayerNewState.Right >= tile.Box.Left + (gameObj.ActivePlayer.Height / 10 - 1) && checkPlayerNewState.Left <= tile.Box.Right - (gameObj.ActivePlayer.Height / 10 - 1))
                {
                    gameObj.InventoryContainer[tile.ItemKey].Collected = true;
                    tile.Collected = true;
                }
                //Bottom
                if (checkPlayerNewState.Top <= tile.Box.Bottom + gameObj.ActivePlayer.Height - physicsEngine.YOffSet - 2 && checkPlayerNewState.Top >= tile.Box.Bottom - 1 && checkPlayerNewState.Right >= tile.Box.Left + (gameObj.ActivePlayer.Height / 10 - 1) && checkPlayerNewState.Left <= tile.Box.Right - (gameObj.ActivePlayer.Height / 10 - 1))
                {
                    gameObj.InventoryContainer[tile.ItemKey].Collected = true;
                    tile.Collected = true;
                }
            }
            return false;
        }
        public bool ladderCollision(Rectangle player)
        {
            foreach (LadderTile tile in gameObj.GenMaps[physicsEngine.ACTIVELEVELFunc].LadderTile)
            {
                if (tile.Box.Intersects(player))
                {
                    return true;
                }
            }
            return false;
        }
    }

}
*/