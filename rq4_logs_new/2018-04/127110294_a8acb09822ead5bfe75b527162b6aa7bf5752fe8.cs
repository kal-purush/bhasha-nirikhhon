using DataAccess.Entity.Entities;

namespace HOApp.Model
{
    class POlineVM : VMBase
    {
        public POline TheEntity
        {
            get
            {
                return (POline)base.theEntity;
            }
            set
            {
                theEntity = value;
                RaisePropertyChanged();
            }
        }
        public POlineVM()
        {
            // Initialise the entity or inserts will fail
            TheEntity = new POline();
        }

    }
}