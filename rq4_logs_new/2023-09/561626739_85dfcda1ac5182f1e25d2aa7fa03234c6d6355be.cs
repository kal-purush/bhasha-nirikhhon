using System;
using System.Collections.Generic;
using System.Linq;
using System.Web.UI.WebControls;

///<summary>
/// 訂單紀錄組件
///</summary>
namespace Web.Transaction
{

    /// <summary>
    /// 商品資料 資料結構
    /// </summary>
    [Serializable]
    class TransactionCommodity
    {
        /// <summary>
        /// 商品資料 資料結構
        /// </summary>
        public string CT_Name;
        public int CT_Id;
        public int CT_Price;
        public string CT_Thumbnail;
        public int TRT_Num;

        /// <summary>
        /// 商品資料 建構子
        /// </summary>
        /// <param name="CT_Name">商品名稱</param>
        /// <param name="CT_Id">商品ID</param>
        /// <param name="CT_Price">商品單價</param>
        /// <param name="CT_Thumbnail">商品縮圖</param>
        /// <param name="TRT_Num">商品選購數量</param>
        public TransactionCommodity(
            string CT_Name,
            string CT_Id,
            string CT_Price,
            string CT_Thumbnail,
            string TRT_Num
        ){
            this.CT_Name = CT_Name;
            this.CT_Id = int.Parse(CT_Id);
            this.CT_Price = int.Parse(CT_Price);
            this.CT_Thumbnail = CT_Thumbnail;
            this.TRT_Num = int.Parse(TRT_Num);
        }
    }

    /// <summary>
    /// 訂單資料 資料結構
    /// </summary>
    [Serializable]
    class TransactionRecord
    {
        /// <summary>
        /// 訂單資料 資料結構
        /// </summary>
        public string TT_Id;
        public string TT_Date;
        public List<TransactionCommodity> Commoditys = new List<TransactionCommodity>();

        /// <summary>
        /// 訂單資料 建構子
        /// </summary>
        /// <param name="TT_Id">訂單ID</param>
        /// <param name="TT_Date">訂單成立時間</param>
        public TransactionRecord(
            string TT_Id,
            string TT_Date
        )
        {
            this.TT_Id = TT_Id;
            this.TT_Date = TT_Date;
        }
    }

    /// <summary>
    /// 商品項目UI
    /// </summary>
    class TransactionCommodityUI : Panel
    {
        /// <summary>
        /// 連結
        /// </summary>
        LinkButton link = new LinkButton();
        /// <summary>
        /// 縮圖
        /// </summary>
        Image thumbnail = new Image();
        /// <summary>
        /// 商品名稱
        /// </summary>
        Label name = new Label();
        /// <summary>
        /// 商品ID
        /// </summary>
        Label id = new Label();

        /// <summary>
        /// 分隔線
        /// </summary>
        Panel hr = new Panel();

        /// <summary>
        /// 價格(框架)
        /// </summary>
        Panel price_Box = new Panel();
        /// <summary>
        /// 單價
        /// </summary>
        Label price = new Label();
        /// <summary>
        /// 購買數量
        /// </summary>
        Label num = new Label();

        /// <summary>
        /// 商品項目UI 建構子
        /// </summary>
        /// <param name="commodity">商品資料</param>
        public TransactionCommodityUI(TransactionCommodity commodity)
        {
            //UI Css掛載
            this.CssClass = "CT_Item";
            //商品資料初始化
            thumbnail.CssClass = "CT_Thumbnail";
            thumbnail.ImageUrl = commodity.CT_Thumbnail;
            name.CssClass = "CT_Name";
            name.Text = commodity.CT_Name;
            id.CssClass = "CT_Id";
            id.Text = $"{commodity.CT_Id}";
            //掛載連結
            link.CssClass = "CT_LinkBox";
            link.Attributes.Add(
                "href", 
                $"commodity/Item.aspx?commodityId={commodity.CT_Id}"
            );
            link.Controls.Add(thumbnail);
            link.Controls.Add(name);
            link.Controls.Add(id);

            //分隔線
            hr.CssClass = "TT_hr";

            //價格資料
            price_Box.CssClass = "CT_Price_Box";
            num.CssClass = "CT_PriceNum";
            num.Text = $"{commodity.TRT_Num}";
            price.CssClass = "CT_Price";
            price.Text = $"${commodity.CT_Price.ToString("N0")}";
            //掛載至子UI
            price_Box.Controls.Add(num);
            price_Box.Controls.Add(price);

            //掛載至主UI
            this.Controls.Add(link);
            this.Controls.Add(hr);
            this.Controls.Add(price_Box);
        }
    }

    /// <summary>
    /// 訂單項目UI
    /// </summary>
    class TransactionRecordUI : Panel
    {
        /// <summary>
        /// 訂單資料(框架)
        /// </summary>
        Panel id_Box = new Panel();
        /// <summary>
        /// 訂單ID
        /// </summary>
        Label id = new Label();
        /// <summary>
        /// 訂單成立時間
        /// </summary>
        Label date = new Label();
        /// <summary>
        /// 訂單商品列表
        /// </summary>
        Panel commodityList = new Panel();
        /// <summary>
        /// 訂單小計(框架)
        /// </summary>
        Panel subTotal_Box = new Panel();
        /// <summary>
        /// 訂單小計
        /// </summary>
        Label subTotal = new Label();

        /// <summary>
        /// 分隔線
        /// </summary>
        Panel hr_1 = new Panel();
        /// <summary>
        /// 分隔線
        /// </summary>
        Panel hr_2 = new Panel();
        /// <summary>
        /// 分隔線
        /// </summary>
        Panel hr_3 = new Panel();

        /// <summary>
        /// 訂單項目UI 建構子
        /// </summary>
        /// <param name="record">訂單資料</param>
        public TransactionRecordUI(TransactionRecord record)
        {
            ///價格小計
            long subTotalNum = 0;
            ///訂單成立時間資料
            String transactionDateText = record.TT_Date.Remove(10, 1);
            transactionDateText = transactionDateText.Insert(10, "<br>");
            //UI Css掛載
            this.CssClass = "TT_Item";

            //訂單資料
            id_Box.CssClass = "TT_TextId_Box";
            id.CssClass = "TT_TextId";
            id.Text = record.TT_Id;
            id_Box.Controls.Add(id);
            date.CssClass = "TT_TextDate";
            date.Text = transactionDateText;

            //分隔線
            hr_1.CssClass = "TT_hr";
            hr_2.CssClass = "TT_hr";
            hr_3.CssClass = "TT_hr";

            //商品列表初始化
            commodityList.CssClass = "CT_ListBox";
            int row = 0;
            foreach (TransactionCommodity item in record.Commoditys)
            {
                //判斷分隔線
                if(record.Commoditys.First() != item)
                {
                    Panel hr = new Panel();
                    hr.CssClass = "CT_hr";
                    commodityList.Controls.Add(hr);
                }
                //掛載商品列UI
                commodityList.Controls.Add(
                    new TransactionCommodityUI(item));
                //累計價格
                subTotalNum += item.CT_Price * item.TRT_Num;
                row++;
            }

            //小計資料
            subTotal_Box.CssClass = "TT_SubTotal_Box";
            subTotal.CssClass = "TT_SubTotal";
            subTotal.Text = $"${subTotalNum.ToString("N0")}";
            //掛載至子UI
            subTotal_Box.Controls.Add(subTotal);

            //掛載至主UI
            this.Controls.Add(id_Box);
            this.Controls.Add(hr_1);
            this.Controls.Add(date);
            this.Controls.Add(hr_2);
            this.Controls.Add(commodityList);
            this.Controls.Add(hr_3);
            this.Controls.Add(subTotal_Box);
        }
    }
}