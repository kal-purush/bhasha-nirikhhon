using System;
using System.Linq;
using WardenAnd23Prisoners.Domain.Room;

namespace WardenAnd23Prisoners.Domain.Person
{
    public class Prisoner : PersonOnShip, IPrisoner
    {
        IDomainIdentifier Identifier;
        int NumberOfTimesToFlipFirstSwitch;
        int CountOfTheNumbersFirstSwitchFlipped;
        SwitchPosition FlipFirstSwitchPostion;

        public Prisoner(
            IDomainIdentifier identifier,
            PersonOnShipRole personOnShipRole = PersonOnShipRole.Prisoner,
            int numberOfTimesToFlipFirstSwitch = 2,
            SwitchPosition flipFirstSwitchPostion = SwitchPosition.Down
            ) : base(personOnShipRole)
        {
            Identifier = identifier;
            NumberOfTimesToFlipFirstSwitch = numberOfTimesToFlipFirstSwitch;
            FlipFirstSwitchPostion = flipFirstSwitchPostion;
        }

        public IDomainIdentifier GetPrisonerIdentifier()
        {
            return Identifier;
        }

        public virtual void Action(ISwitchRoom switchRoom, Notify Announce)
        {
            if (CountOfTheNumbersFirstSwitchFlipped != NumberOfTimesToFlipFirstSwitch)
            {
                ISwitch firstSwitch = switchRoom.GetSwithes().Where(s => Convert.ToInt32(s.GetSwitchIdentifier().GetIdentifier()).Equals(1)).Select(s => s).First();

                if (firstSwitch.GetSwitchPosition() == FlipFirstSwitchPostion)
                {
                    firstSwitch.FlipSwitchPosition();
                    CountOfTheNumbersFirstSwitchFlipped++;
                    return;
                }
            }

            ISwitch secondSwitch = switchRoom.GetSwithes().Where(s => Convert.ToInt32(s.GetSwitchIdentifier().GetIdentifier()).Equals(2)).Select(s => s).First();
            secondSwitch.FlipSwitchPosition();
        }

        public int GetCountOfTheNumbersFirstSwitchFlipped()
        {
            return CountOfTheNumbersFirstSwitchFlipped;
        }

        public int GetNumberOfTimesToFlipFirstSwitch()
        {
            return NumberOfTimesToFlipFirstSwitch;
        }
    }
}