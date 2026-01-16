using UnityEngine;
using System.Collections;
using UnityStandardAssets.Cameras;

namespace CRAG.InputSystem
{
    public class ZoomCamCommand : ICommand<IsometricCamera>
    {
        /// <summary>Значение оси колесика мыши, передаваемое классом Input</summary>
        public float scroll;
        
        /// <summary>
        /// Конструктор класса
        /// </summary>
        /// <param name="scroll">Значение оси колесика мыши, передаваемое классом Input</param>
        public ZoomCamCommand(float scroll)
        {
            this.scroll = scroll;
        }

        public void Execute(IsometricCamera actor)
        {
            actor.Zoom(scroll);
        }
    }
}
