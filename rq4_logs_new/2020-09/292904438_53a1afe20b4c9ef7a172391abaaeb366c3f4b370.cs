
namespace Ex03.GarageLogic
{
    public class GarageCard
    {
        public enum eStatus
        {
            InRepair,
            Fixed,
            paidFor
        }

        private Car m_CarToFix;
        private readonly string r_OwnersName;
        private readonly string r_PhoneNumber;
        private eStatus m_StatusInGarage;

        GarageCard(string i_OwnersName, string i_PhoneNumber, Car i_CarToFix)
        {
            r_OwnersName = i_OwnersName;
            r_PhoneNumber = i_PhoneNumber;
            m_CarToFix = i_CarToFix;
            m_StatusInGarage = eStatus.InRepair; 
        }

        //Properties
        public string OwnersName
        {
            get
            {
                return r_OwnersName;
            }
        }
        public string PhoneNumber
        {
            get
            {
                return r_PhoneNumber;
            }
        }

        public Car CarToFix
        {
            get
            {
                return m_CarToFix;
            }
        }

        public eStatus Status
        {
            get
            {
                return m_StatusInGarage;
            }

            set
            {
                m_StatusInGarage = value;
            }
        }


    }
}