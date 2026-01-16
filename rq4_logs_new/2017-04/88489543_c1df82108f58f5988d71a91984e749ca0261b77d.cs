using System;

namespace PXC.Model
{
    ///<summary>
    ///C_RoomEntity实体类(房屋)
    ///</summary>
    [Serializable]
    public partial class C_RoomEntity
    {
        #region "Private Variables"

        private String _DataTable_Action_;  // 操作方法 Insert:增加 Update:修改 Delete:删除
        private Int32 _RoomID=0; // 房屋编号
        private Int32 _CommunityID=0; // 楼盘编号
        private Int32 _BuildingID=0; // 楼栋编号
        private String _UnitNo = ""; // 单元号
        private String _RoomNo=""; // 房号
        private Int32 _RealFloor = 0; // 实际层
        private Int32 _PhysicalFloor=0; // 物理层
        private Int32 _RoomType=0; // 户型编号
        private Decimal _GrossFloorArea=0; // 建筑面积
        private Int32 _AspectType=0; // 朝向类型编号
        private Int32 _LandscapeType=0; // 景观类型编号
        private Decimal _UnitPrice=0; // 单价
        private Decimal _TotalPrice=0; // 总价
        private Decimal _PriceCoefficient=0; // 价格系数
        private Int32 _RoomLayoutType=0; // 户型结构编号
        private Int32 _RoomPurpose=0; // 房屋用途编号
        private Int32 _IsCanEstimate=0; // 是否可估(0未知1是2否)
        private Int32 _IsAreaConfirm=0; // 面积是否确认(0未知1是2否)
        private Int32 _UnitConstractionArea=0; // 套内面积
        private Int32 _AppendantType=0; // 附属房屋类型编号
        private Decimal _AppendantArea=0; // 附属房屋面积
        private Int32 _WindLightType=0; // 通风采光类型编号
        private Int32 _NoiseType = 0; // 噪音情况编号
        private Int32 _DecorateType=0; // 装修类型编号
        private Int32 _IsHaveKitchen=0; // 是否有厨房(0未知1是2否)
        private Int32 _BalconyCount=0; // 阳台数:
        private Int32 _BathroomCount=0; // 洗手间数
        private String _RoomRemark=""; // 备注
        private Int32 _InsertUser=0; // 添加人的UserID
        private DateTime _InsertTime; // 添加时间
        private Int32 _UpdateUser=0; // 最后修改人的UserID
        private DateTime _UpdateTime; // 最后修改时间
        private Int32 _IsDel=0; // 是否已删除(1是0否)

        #endregion

        #region "Public Variables"

        ///<summary>
        /// 操作方法 Insert:增加 Update:修改 Delete:删除
        ///</summary>
        public String DataTable_Action_
        {
            set { this._DataTable_Action_ = value; }
            get { return this._DataTable_Action_; }
        }
        /// <summary>
        /// 房屋编号
        /// </summary>
        public Int32  RoomID
        {
            set { this._RoomID = value; }
            get { return this._RoomID; }
        }
            
        /// <summary>
        /// 楼盘编号
        /// </summary>
        public Int32  CommunityID
        {
            set { this._CommunityID = value; }
            get { return this._CommunityID; }
        }
            
        /// <summary>
        /// 楼栋编号
        /// </summary>
        public Int32  BuildingID
        {
            set { this._BuildingID = value; }
            get { return this._BuildingID; }
        }

        /// <summary>
        /// 单元号
        /// </summary>
        public String UnitNo
        {
            set { this._UnitNo = value; }
            get { return this._UnitNo; }
        }
            
        /// <summary>
        /// 房号
        /// </summary>
        public String  RoomNo
        {
            set { this._RoomNo = value; }
            get { return this._RoomNo; }
        }
            
        /// <summary>
        /// 实际层
        /// </summary>
        public Int32 RealFloor
        {
            set { this._RealFloor = value; }
            get { return this._RealFloor; }
        }
            
        /// <summary>
        /// 物理层
        /// </summary>
        public Int32  PhysicalFloor
        {
            set { this._PhysicalFloor = value; }
            get { return this._PhysicalFloor; }
        }
            
        /// <summary>
        /// 户型编号
        /// </summary>
        public Int32  RoomType
        {
            set { this._RoomType = value; }
            get { return this._RoomType; }
        }
            
        /// <summary>
        /// 建筑面积
        /// </summary>
        public Decimal  GrossFloorArea
        {
            set { this._GrossFloorArea = value; }
            get { return this._GrossFloorArea; }
        }
            
        /// <summary>
        /// 朝向类型编号
        /// </summary>
        public Int32  AspectType
        {
            set { this._AspectType = value; }
            get { return this._AspectType; }
        }
            
        /// <summary>
        /// 景观类型编号
        /// </summary>
        public Int32  LandscapeType
        {
            set { this._LandscapeType = value; }
            get { return this._LandscapeType; }
        }
            
        /// <summary>
        /// 单价
        /// </summary>
        public Decimal  UnitPrice
        {
            set { this._UnitPrice = value; }
            get { return this._UnitPrice; }
        }
            
        /// <summary>
        /// 总价
        /// </summary>
        public Decimal  TotalPrice
        {
            set { this._TotalPrice = value; }
            get { return this._TotalPrice; }
        }
            
        /// <summary>
        /// 价格系数
        /// </summary>
        public Decimal  PriceCoefficient
        {
            set { this._PriceCoefficient = value; }
            get { return this._PriceCoefficient; }
        }
            
        /// <summary>
        /// 户型结构编号
        /// </summary>
        public Int32  RoomLayoutType
        {
            set { this._RoomLayoutType = value; }
            get { return this._RoomLayoutType; }
        }
            
        /// <summary>
        /// 房屋用途编号
        /// </summary>
        public Int32  RoomPurpose
        {
            set { this._RoomPurpose = value; }
            get { return this._RoomPurpose; }
        }
            
        /// <summary>
        /// 是否可估(0未知1是2否)
        /// </summary>
        public Int32  IsCanEstimate
        {
            set { this._IsCanEstimate = value; }
            get { return this._IsCanEstimate; }
        }
            
        /// <summary>
        /// 面积是否确认(0未知1是2否)
        /// </summary>
        public Int32  IsAreaConfirm
        {
            set { this._IsAreaConfirm = value; }
            get { return this._IsAreaConfirm; }
        }
            
        /// <summary>
        /// 套内面积
        /// </summary>
        public Int32  UnitConstractionArea
        {
            set { this._UnitConstractionArea = value; }
            get { return this._UnitConstractionArea; }
        }
            
        /// <summary>
        /// 附属房屋类型编号
        /// </summary>
        public Int32  AppendantType
        {
            set { this._AppendantType = value; }
            get { return this._AppendantType; }
        }
            
        /// <summary>
        /// 附属房屋面积
        /// </summary>
        public Decimal  AppendantArea
        {
            set { this._AppendantArea = value; }
            get { return this._AppendantArea; }
        }
            
        /// <summary>
        /// 通风采光类型编号
        /// </summary>
        public Int32  WindLightType
        {
            set { this._WindLightType = value; }
            get { return this._WindLightType; }
        }

        /// <summary>
        /// 噪音情况编号
        /// </summary>
        public Int32 NoiseType
        {
            set { this._NoiseType = value; }
            get { return this._NoiseType; }
        }
            
        /// <summary>
        /// 装修类型编号
        /// </summary>
        public Int32  DecorateType
        {
            set { this._DecorateType = value; }
            get { return this._DecorateType; }
        }
            
        /// <summary>
        /// 是否有厨房(0未知1是2否)
        /// </summary>
        public Int32  IsHaveKitchen
        {
            set { this._IsHaveKitchen = value; }
            get { return this._IsHaveKitchen; }
        }
            
        /// <summary>
        /// 阳台数:
        /// </summary>
        public Int32  BalconyCount
        {
            set { this._BalconyCount = value; }
            get { return this._BalconyCount; }
        }
            
        /// <summary>
        /// 洗手间数
        /// </summary>
        public Int32  BathroomCount
        {
            set { this._BathroomCount = value; }
            get { return this._BathroomCount; }
        }
            
        /// <summary>
        /// 备注
        /// </summary>
        public String  RoomRemark
        {
            set { this._RoomRemark = value; }
            get { return this._RoomRemark; }
        }
            
        /// <summary>
        /// 添加人的UserID
        /// </summary>
        public Int32  InsertUser
        {
            set { this._InsertUser = value; }
            get { return this._InsertUser; }
        }
            
        /// <summary>
        /// 添加时间
        /// </summary>
        public DateTime  InsertTime
        {
            set { this._InsertTime = value; }
            get { return this._InsertTime; }
        }
            
        /// <summary>
        /// 最后修改人的UserID
        /// </summary>
        public Int32  UpdateUser
        {
            set { this._UpdateUser = value; }
            get { return this._UpdateUser; }
        }
            
        /// <summary>
        /// 最后修改时间
        /// </summary>
        public DateTime  UpdateTime
        {
            set { this._UpdateTime = value; }
            get { return this._UpdateTime; }
        }
            
        /// <summary>
        /// 是否已删除(1是0否)
        /// </summary>
        public Int32  IsDel
        {
            set { this._IsDel = value; }
            get { return this._IsDel; }
        }
            
        #endregion
    }
}
  