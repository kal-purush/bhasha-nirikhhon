using DevExpress.ExpressApp.DC;
using DevExpress.Xpo;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DuyKhanhHR.Module.BusinessObjects
{
    [Persistent(@"BoPhan")]
    [DefaultProperty("tenBoPhan")]
    [XafDisplayName("Bộ Phận")]
    public class BoPhan : XPLiteObject
    {
        public BoPhan(Session session) : base(session) { }
        public override void AfterConstruction()
        {
            base.AfterConstruction();
        }
        int fId;
        [Key(true)]
        [XafDisplayName("STT")]
        public int Id
        {
            get { return fId; }
            set { SetPropertyValue("Id", ref fId, value); }
        }
        string fMaBoPhan;
        [XafDisplayName("Mã Bộ Phận")]
        public string maBoPhan
        {
            get { return fMaBoPhan; }
            set { SetPropertyValue("maBoPhan", ref fMaBoPhan, value); }
        }
        string fTenBoPhan;
        [XafDisplayName("Tên Bộ Phận")]
        public string tenBoPhan
        {
            get { return fTenBoPhan; }
            set { SetPropertyValue("fTenBoPhan", ref fTenBoPhan, value); }
        }
        NhanVien fTruongBoPhan;
        [XafDisplayName("Trưởng Bộ Phận")]
        public NhanVien truongBoPhan
        {
            get { return fTruongBoPhan; }
            set { SetPropertyValue("truongBoPhan", ref fTruongBoPhan, value); }
        }
        string fGhiChu;
        [XafDisplayName("Ghi Chú")]
        public string ghiChu
        {
            get { return fGhiChu; }
            set { SetPropertyValue("ghiChu", ref fGhiChu, value); }
        }
        [Association(@"NhanVien-BoPhan")]
        [XafDisplayName("Danh Sách Nhân Viên")]
        public XPCollection<NhanVien> NhanViens { get { return GetCollection<NhanVien>("NhanViens"); } }
        [Association(@"ThangChamCongs-BoPhan")]
        [XafDisplayName("Tháng Chấm Công")]
        public XPCollection<ThangChamCong> thangChamCongs { get { return GetCollection<ThangChamCong>("thangChamCongs"); } }
    }
}