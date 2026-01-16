using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ConasiCRM.Portable.Models
{
    public class StatusCodeActivity
    {
        public static StatusCodeModel GetStatusCodeById(string statusCodeId)
        {
            return StatusCodes().SingleOrDefault(x => x.Id == statusCodeId);
        }

        public static List<StatusCodeModel> StatusCodes()
        {
            return new List<StatusCodeModel>()
            {
                new StatusCodeModel("1","Hoàn Thành","#06CF79"),
                new StatusCodeModel("0","Đang Mở","#03ACF5"),
                new StatusCodeModel("2","Đã hủy","#FA7901"),
        //        new StatusCodeModel("3","Scheduled","#FA7901"),
            };
        }
    }
}