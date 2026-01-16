using GPRO.Ultilities;
using PMS.Business.Models;
using QLKeHoach.Data.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QLKeHoach.Data.BLL
{
   public class BLLCurrencyUnit
    {
        static Object key = new object();
        private static volatile BLLCurrencyUnit _Instance;
        public static BLLCurrencyUnit Instance
        {
            get
            {
                if (_Instance == null)
                    lock (key)
                        _Instance = new BLLCurrencyUnit();

                return _Instance;
            }
        }

        private BLLCurrencyUnit() { }

        public List<CurrencyUnitModel> Gets()
        {
            using (var db = new GproPlanEntities())
            {
                try
                {
                    return (from x in db.P_CurrencyUnit
                            where !x.IsDeleted
                            select new CurrencyUnitModel()
                            {
                                Id = x.Id,
                                Code = x.Code,
                                Note = x.Note
                            }).ToList();
                }
                catch (Exception)
                {
                }
                return new List<CurrencyUnitModel>();
            }
        }

        public List<ModelSelectItem> GetSelects()
        {
            using (var db = new GproPlanEntities())
            {
                try
                {
                    return (from x in db.P_CurrencyUnit
                            where !x.IsDeleted
                            select new ModelSelectItem()
                            {
                                Id = x.Id,
                                Name = x.Code  
                            }).ToList();
                }
                catch (Exception)
                {
                }
                return new List<ModelSelectItem>();
            }
        }

        public ResponseBase Delete(int Id)
        {
            var result = new ResponseBase();
            try
            {
                using (var db = new GproPlanEntities())
                {
                    var obj = db.P_CurrencyUnit.FirstOrDefault(x => x.Id == Id);
                    if (obj != null)
                    {
                        obj.IsDeleted = true;
                        obj.CreatedDate = DateTime.Now;
                        db.SaveChanges();
                        result.IsSuccess = true;
                        result.Messages.Add(new Message() { msg = "Xóa đơn vị tiền tệ thành công.", Title = "Thông Báo" });
                    }
                    else
                    {
                        result.IsSuccess = true;
                        result.Messages.Add(new Message() { msg = "Không tìm thấy thông tin đơn vị tiền tệ . Xóa đơn vị tiền tệ thất bại.", Title = "Lỗi CSDL" });
                    }
                }
            }
            catch (Exception)
            {
                result.IsSuccess = true;
                result.Messages.Add(new Message() { msg = "Không tìm thấy thông tin đơn vị tiền tệ . Xóa đơn vị tiền tệ thất bại.", Title = "Lỗi Exception" });
            }
            return result;
        }

        public ResponseBase InsertOrUpdate(CurrencyUnitModel objModel)
        {
            var rs = new ResponseBase();
            using (var db = new GproPlanEntities())
            {
                try
                {
                    if (CheckExists(objModel,  db) != null)
                    {
                        rs.IsSuccess = false;
                        rs.Messages.Add(new Message() { msg = "Mã đơn vị tiền tệ đã tồn tại. Vui lòng chọn lại mã khác", Title = "Lỗi trùng tên" });
                    } 
                    else
                    {
                        P_CurrencyUnit CurrencyUnition;
                        if (objModel.Id == 0)
                        {
                            CurrencyUnition = new P_CurrencyUnit();
                            Parse.CopyObject(objModel, ref CurrencyUnition);
                            CurrencyUnition.CreatedDate = DateTime.Now;
                            db.P_CurrencyUnit.Add(CurrencyUnition);
                            rs.IsSuccess = true;
                        }
                        else
                        {
                            CurrencyUnition = db.P_CurrencyUnit.FirstOrDefault(x => !x.IsDeleted && x.Id == objModel.Id);
                            if (CurrencyUnition != null)
                            {
                                CurrencyUnition.Code = objModel.Code;
                                CurrencyUnition.Note = objModel.Note; 
                                CurrencyUnition.UpdatedDate = DateTime.Now;
                                rs.IsSuccess = true;
                            }
                            else
                            {
                                rs.IsSuccess = false;
                                rs.Messages.Add(new Message() { msg = "đơn vị tiền tệ đang thao tác không tồn tại hoặc đã bị xóa. Vui lòng chọn lại tên khác", Title = "Lỗi trùng tên" });
                            }
                        }
                        if (rs.IsSuccess)
                        {
                            db.SaveChanges();
                            rs.IsSuccess = true;
                        }
                    }
                }
                catch (Exception ex)
                {
                }
            }
            return rs;
        }

        private P_CurrencyUnit CheckExists(CurrencyUnitModel objModel, GproPlanEntities db)
        { 
                return db.P_CurrencyUnit.FirstOrDefault(x => !x.IsDeleted && x.Id != objModel.Id && x.Code.Trim().Equals(objModel.Code));
        }
    }
}