using fix_it_tracker_back_end.Data.Repositories;

namespace fix_it_tracker_back_end.Model.BindingTargets
{
    public class ItemData
    {
        public string Serial { get; set; }

        public int ItemType { get; set; }

        public Item Item => new Item
        {
            Serial = Serial,
            ItemType = ItemType == 0 ? null : new ItemType
            {
                ItemTypeID = ItemType,
                Name = "Name",
                Model = "Model",
                Manufacturer = "Manufacturer"
            }
        };
    }
}