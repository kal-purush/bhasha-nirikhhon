using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using SupplyChainManagement.Models;
using SupplyChainManagement.Models.ItemManagement;

namespace SupplyChainManagement.Data
{
    public class DataSourceMock : DataSource
    {
        private Dictionary<int, Item> _Items = new Dictionary<int, Item>();
        public Dictionary<int, Item> Items
        {
            get
            {
                return _Items;
            }
        }

        private Dictionary<int, Workplace> _Workplaces = new Dictionary<int, Workplace>();
        public Dictionary<int, Workplace> Workplaces
        {
            get
            {
                return _Workplaces;
            }
        }

        private Dictionary<int, ItemJob> _ItemJobs = new Dictionary<int, ItemJob>();
        public Dictionary<int, ItemJob> ItemJobs
        {
            get
            {
                return _ItemJobs;
            }
        }

        public DataSourceMock()
        {

        }

        /// <summary>
        /// Adds a new item
        /// </summary>
        /// <param name="item"></param>
        public void AddNewItem(Item item)
        {
            Items.Add(item.Id, item);
        }

        /// <summary>
        /// Adds a new workplace
        /// </summary>
        /// <param name="workplace"></param>
        public void AddNewWorkplace(Workplace workplace)
        {
            Workplaces.Add(workplace.Id, workplace);
        }

        /// <summary>
        /// Adds a new item job
        /// </summary>
        /// <param name="job"></param>
        public void AddNewItemJob(ItemJob job)
        {
            if (job.Workplace != null)
            {
                job.Workplace.Jobs.Add(job);
            }

            ItemJobs.Add(job.Id, job);
        }
    }
}