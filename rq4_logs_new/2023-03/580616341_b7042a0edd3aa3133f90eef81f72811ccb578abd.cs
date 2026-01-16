
// Copyright (C) 2023 Nicholas Maltbie
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
// associated documentation files (the "Software"), to deal in the Software without restriction,
// including without limitation the rights to use, copy, modify, merge, publish, distribute,
// sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all copies or
// substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING
// BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
// CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
// ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

using System;
using nickmaltbie.OpenKCC.Character;
using nickmaltbie.OpenKCC.Character.Config;
using nickmaltbie.OpenKCC.Character.Events;
using nickmaltbie.OpenKCC.Utils;
using nickmaltbie.Treachery.Interactive.Stamina;
using nickmaltbie.Treachery.Player;
using UnityEngine.InputSystem;

namespace nickmaltbie.Treachery.Action.PlayerActions
{
    /// <summary>
    /// Sprint action performed by the player
    /// </summary>
    [Serializable]
    public class SprintAction : ContinuousConditionalAction<PlayerAction>
    {
        private IMovementActor player;
        private KCCMovementEngine movementEngine;

        public SprintAction(
            InputActionReference actionReference,
            IActionActor<PlayerAction> actor,
            KCCMovementEngine movementEngine,
            IMovementActor player,
            IStaminaMeter stamina,
            float staminaCostRate,
            float staminaRequiredToStart)
            : base(actionReference, actor, stamina, PlayerAction.Sprint, staminaCostRate, staminaRequiredToStart, StartSprintEvent.Instance, StopSprintEvent.Instance)
        {
            this.player = player;
            this.movementEngine = movementEngine;
        }

        protected override bool Condition()
        {
            KCCGroundedState grounded = movementEngine.GroundedState;
            return base.Condition() &&
                player.InputMovement.magnitude > KCCUtils.Epsilon &&
                grounded.StandingOnGround && !grounded.Sliding;
        }
    }
}