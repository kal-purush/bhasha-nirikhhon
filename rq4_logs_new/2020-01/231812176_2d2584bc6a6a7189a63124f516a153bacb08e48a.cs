using StardewModdingAPI;
using StardewModdingAPI.Events;
using StardewValley;

namespace Sanic
{
    public class SprintHelper
    {
        public ModConfig Config { get; private set; }

        private bool isSprinting = false;

        public SprintHelper(ModConfig config, IModHelper helper)
        {
            Config = config;
            helper.Events.GameLoop.UpdateTicking += OnUpdateTicking;
        }

        public void OnUpdateTicking(object _, UpdateTickingEventArgs args)
        {
            // stop sprinting immediately and bail out if the run/walk button is held
            if (IsRunWalkButtonHeld() && isSprinting)
            {
                isSprinting = false;
                return;
            }

            // set speed to sprint speed if we are sprinting and speed is less than sprint speed
            if (isSprinting && Game1.player.Speed < Config.SprintSpeed)
            {
                Game1.player.Speed = Config.SprintSpeed;
            }

            // remove some stamina if we have been sprinting for long enough
            if (args.Ticks % Config.StaminaLossTickCount == 0)
            {
                Game1.player.Stamina -= Game1.player.MaxStamina * Config.StaminaLossModifier;
            }
        }

        public void OnSprintKeyPressed(object _, ButtonPressedEventArgs args)
        {
            // sprint only if the player is not currently holding the run/walk button
            if (IsRunWalkButtonHeld())
                return;

            isSprinting = true;
        }

        public void OnSprintKeyReleased(object _, ButtonReleasedEventArgs args)
        {
            // no matter what, when sprint key is released, stop sprinting.
            isSprinting = false;
        }

        public bool IsRunWalkButtonHeld()
        {
            // run button is NOT being held when all of the assigned run keys are up
            return !Game1.areAllOfTheseKeysUp(Game1.GetKeyboardState(), Game1.options.runButton);
        }
    }
}