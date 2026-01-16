#pragma warning disable CS0626 // Method, operator, or accessor is marked external and has no attributes on it

using Microsoft.Xna.Framework;
using Monocle;
using MonoMod;

namespace Celeste
{
    class patch_MusicFadeTrigger : MusicFadeTrigger
    {

        private PositionModes? positionMode;

        public patch_MusicFadeTrigger(EntityData data, Vector2 offset)
            : base(data, offset) {
            // no-op. MonoMod ignores this - we only need this to make the compiler shut up.
        }

        public extern void orig_ctor(EntityData data, Vector2 offset);
        [MonoModConstructor]
        public void ctor(EntityData data, Vector2 offset) {
            orig_ctor(data, offset);

            string attr = data.Attr("positionMode");
            if (!string.IsNullOrEmpty(attr) && System.Enum.TryParse(attr.ToString(), ignoreCase: true, out PositionModes result))
                positionMode = result;
        }

        [MonoModReplace]
        public override void OnStay(Player player)
        {
            float value;
            if (positionMode.HasValue)
                value = Calc.ClampedMap(GetPositionLerp(player, positionMode.Value), 0f, 1f, FadeA, FadeB);
            else
                value = (!LeftToRight) ? Calc.ClampedMap(player.Center.Y, Top, Bottom, FadeA, FadeB) : Calc.ClampedMap(player.Center.X, Left, Right, FadeA, FadeB);

            if (string.IsNullOrEmpty(Parameter))
                Audio.SetMusicParam("fade", value);
            else
                Audio.SetMusicParam(Parameter, value);
        }

    }
}