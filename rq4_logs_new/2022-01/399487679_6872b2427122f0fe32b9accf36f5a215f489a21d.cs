using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Igtampe.Neco.Common {
    
    /// <summary>Any item that can have a date (Like a date created or date updated)</summary>
    public interface Dateable {

        /// <summary>Date of this item's creation</summary>
        public DateTime DateCreated { get; set; }

        /// <summary>Date of the last item's update</summary>
        public DateTime DateUpdated { get; set; }

    }
}