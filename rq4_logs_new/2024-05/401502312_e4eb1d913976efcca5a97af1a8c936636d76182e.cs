using Backbone.Graphics;
using Microsoft.Xna.Framework;
using System;
using System.Collections.Generic;
using System.Reflection;

namespace Backbone.Actions
{
    internal class ChangeMovableSettingsAction : IAction3D
    {
        public List<IAction3D> SubActions { get; set; }

        MovableSettings movableSettings;

        public ChangeMovableSettingsAction(MovableSettings movableSettings)
        {
            this.movableSettings = movableSettings;
        }

        public void Reset()
        {
        }

        public bool Update(Movable3D movable, GameTime gameTime)
        {
            movable.Model = movableSettings.Model;
            movable.MeshProperties = movableSettings.MeshProperties;

            var objType = movable.GetType();

            foreach (var kvp in movableSettings.Properties)
            {
                if(kvp.Key.ToUpper() == "ALPHA")
                {
                    movable.SetAlpha((float)kvp.Value);
                } else
                {
                    PropertyInfo propertyInfo = objType.GetProperty(kvp.Key);
                    propertyInfo.SetValue(movable, Convert.ChangeType(kvp.Value, propertyInfo.PropertyType), null);
                }
            }

            return true;
        }
    }
}