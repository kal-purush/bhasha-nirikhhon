using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gestion_Taller {
    public class InventoryItem {
        public int Id { get; private set; }
        public String Name { get; private set; }
        public String Description { get; private set; }
        public int Icon_id { get; private set; }

        private int userIdUsing;
        public int UserIdUsing {
            get {
                return userIdUsing;
            } set {
                DBConnection.Instance.SetInventoryItemUser(this.Id, value);
                userIdUsing = value;
            }
        }

        public InventoryItem(int id, String name, String description, int userIdUsing, int icon_id) {
            this.Id = id;
            this.Name = name;
            this.Description = description;
            this.UserIdUsing = userIdUsing;
            this.Icon_id = icon_id;
        }
    }
}