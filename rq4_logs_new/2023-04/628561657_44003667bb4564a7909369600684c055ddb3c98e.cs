using JKEY_INTERNAL.Models;
using JKEY_INTERNAL.Models.Attributes;
using JKEY_INTERNAL.Models.CustomModel;
using Microsoft.AspNetCore.Mvc;

namespace JKEY_INTERNAL.Controllers
{
    public class BaseController<T> : Controller
    {

      
        //private 

        public ServiceResponse ValidateRequestData(T record)
        {
            List<string> validateFailed = new List<string>();
            //validate dữ liệu truyền vào
            var properties = typeof(T).GetProperties();

            foreach (var property in properties)//lặp qua các prop để lấy giá trị
            {
                string propertyName = property.Name;//lấy giá trị propName

                var propertyValue = property.GetValue(record);//lấy value của prop

                var isNotNullOrEmptyAttr = (IsNotNullOrEmptyAttribute?)Attribute.GetCustomAttribute(property, typeof(IsNotNullOrEmptyAttribute));

                //nếu có chứa attr và giá trị attr trống
                if (isNotNullOrEmptyAttr != null && string.IsNullOrEmpty(propertyValue?.ToString()))
                {
                    validateFailed.Add(isNotNullOrEmptyAttr.ErrorMessage);
                }


            }
            //nếu tồn tại giá trị validate lỗi
            if ( validateFailed!=null|| validateFailed.Count>0)
            {
                return new ServiceResponse
                {
                    Success = false,

                    Data = validateFailed.FirstOrDefault().ToString()//lấy ra giá trị validate lỗi đầu tiên để gủi sang client


                };
            }
            else//thành công
            {
                return new ServiceResponse
                {
                    Success = true
                };
            }

        }

    }
}