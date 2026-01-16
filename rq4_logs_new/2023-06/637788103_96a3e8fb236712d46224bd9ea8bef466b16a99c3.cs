namespace _Scripts.Powerups
{
    public class Speed : Powerup.Powerup
    {

        private float _speedIncrease;
        
        protected override void Begin()
        {
            _speedIncrease = target.controller.Speed;
            target.controller.Speed += _speedIncrease;
        }

        protected override void End()
        {
            target.controller.Speed -= _speedIncrease;
        }

        protected override void ValuesToCopyToOther(Powerup.Powerup pOther) {}

        protected override void DisplayEffect() {}
    }
}