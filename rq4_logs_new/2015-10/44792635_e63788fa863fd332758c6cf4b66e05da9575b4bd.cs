using Assignment3.Entities;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Assignment3.Utilities
{
    public static class PhysicsUtil
    {

        public static bool CheckCollision(Player player, List<Entity> MazeBlocks)
        {
            if (player.playerModel != null)//make sure player model isn't null
            {
                foreach (Entity e in MazeBlocks)//go through entity list
                {
                    if (e.GetType() == typeof(Wall))//make sure it's a wall
                    {
                        Wall w = (Wall)e;//put it into a wall variable
                        for (int i = 0; i < player.playerModel.Meshes.Count; i++)
                        {
                            BoundingSphere PlayerSphere = player.playerModel.Meshes[i].BoundingSphere;
                            PlayerSphere.Center += player.getPosition();

                            for (int j = 0; j < w.model.Meshes.Count; j++)
                            {
                                BoundingSphere WallSphere = w.model.Meshes[j].BoundingSphere;
                                WallSphere.Center += w.getPosition();

                                if (PlayerSphere.Intersects(WallSphere))
                                {
                                    //collision!
                                    return true;
                                }
                            }
                        }
                    }
                }
            }
            return false;
        }
           
   }
}
