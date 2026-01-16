using WardenAnd23Prisoners.Domain.Room;

namespace WardenAnd23Prisoners.Domain.Person
{
    public class Leader : Prisoner, ILeader
    {
        public Leader(IDomainIdentifier identifier) : base(identifier, PersonOnShipRole.Leader, 44, SwitchPosition.Up)
        { }

        public override void Action(ISwitchRoom switchRoom, Notify Announce)
        {
            base.Action(switchRoom, Announce);

            if (GetCountOfTheNumbersFirstSwitchFlipped().Equals(GetNumberOfTimesToFlipFirstSwitch()))
                MakeAnnouncement(Announce);
        }

        public void MakeAnnouncement(Notify InformWardenToVerify)
        {
            InformWardenToVerify();
        }
    }
}