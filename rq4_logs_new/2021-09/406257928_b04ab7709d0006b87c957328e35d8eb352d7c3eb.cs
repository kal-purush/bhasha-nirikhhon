using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MISA.ApplicationCore.Entities
{
    public class BaseEntity
    {
        #region Property
        /// <summary>
        /// Ngày tạo
        /// Created: LHTDung
        /// Date: 21/9/2021
        /// </summary>
        public DateTime? CreatedDate { get; set; } = DateTime.Now;

        /// <summary>
        /// Người tạo
        /// Created: LHTDung
        /// Date: 21/9/2021
        /// </summary>
        public string CreatedBy { get; set; }

        /// <summary>
        /// Ngày sửa cuối cùng
        /// Created: LHTDung
        /// Date: 21/9/2021
        /// </summary>
        public DateTime? ModifiedDate { get; set; }

        /// <summary>
        /// Người sửa cuối cùng
        /// Created: LHTDung
        /// Date: 21/9/2021
        /// </summary>
        public string ModifiedBy { get; set; }

        #endregion
    }
}