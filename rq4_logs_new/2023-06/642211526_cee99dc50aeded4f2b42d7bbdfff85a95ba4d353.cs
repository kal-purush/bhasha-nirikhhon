using OsEngine.Entity;
using OsEngine.Logging;
using OsEngine.OsTrader.Panels;
using OsEngine.OsTrader.Panels.Attributes;
using OsEngine.OsTrader.Panels.Tab;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Controls.Primitives;
using static System.Windows.Forms.VisualStyles.VisualStyleElement;

namespace OsEngine.OsaExtension.Robots.InterstockArbitrage
{
    [Bot("InterstockArbitrage")]
    internal class InterstockArbitrage : BotPanel
    {
        public InterstockArbitrage(string name, StartProgram startProgram)
            : base(name, startProgram)
        {
            #region  подготовка вкладки ===================================
            // делаем вкладки
            TabCreate(BotTabType.Simple);
            _tab1 = TabsSimple[0];
            _tab1.BestBidAskChangeEvent += _tab1_BestBidAskChangeEvent;
            TabCreate(BotTabType.Simple);
            _tab2 = TabsSimple[1];
            _tab2.BestBidAskChangeEvent += _tab2_BestBidAskChangeEvent;
            //_tab2.CandleFinishedEvent += _tab2_CandleFinishedEvent;
            //TabCreate(BotTabType.Index);
            //_tabIndex = TabsIndex[0];
            //_tabIndex.SpreadChangeEvent += _tabIndex_SpreadChangeEvent;

            Regime = CreateParameter("Regime", "On", new[] { "Off", "On" });
            VolumeInDollars = CreateParameter("VolumeInDollars", 8, 10.0m, 500, 10);






            #endregion конец подготовка вкладки ===================================
        }








        #region TradeLogic ==============================
        private void Trade()
        {
            // знаки после запятой приравняем
            decimal numberOfDigits1 = _tab1.Securiti.DecimalsVolume;
            decimal numberOfDigits2 = _tab2.Securiti.DecimalsVolume;

            if (numberOfDigits1 > numberOfDigits2)
            {
                numberOfDigits1 = numberOfDigits2;
            }
            if (numberOfDigits2 > numberOfDigits1)
            {
                numberOfDigits2 = numberOfDigits1;
            }

            decimal _lotEntrySize = СalculationVolumePosition(_tab1, VolumeInDollars.ValueDecimal);
            //_tab1.SellAtMarket(_lotEntrySize);
            //PrinTextDebug("размер лота _tab1.SellAtMarket " + _lotEntrySize);

            decimal price1sell = _tab1.PriceBestAsk;
            decimal price2buy = _tab2.PriceBestBid;
            // buy1 > sell2
            decimal spread = price2buy - price1sell;

            if (spread > 0.00001m)
            {
                //PrinTextDebug("spread " + spread + '\n'
                //    + " price1sell " + price1sell);


                // buy price1
                // sell price2
                //_tab1.BuyAtLimit(_lotEntrySize, _tab1.PriceBestAsk);
                //_tab2.SellAtLimit(_lotEntrySize, _tab2.PriceBestBid);


                SendNewLogMessage("_lotEntrySize " + _lotEntrySize
                    + '\n'
                    + " _tab1.BuyAtLimit " + price1sell
                    + '\n'
                    + " _tab2.SellAtLimit " + price2buy
                    + '\n'
                    + " spread " + spread, LogMessageType.NoName
                    );
            }




            price1sell = _tab1.PriceBestAsk;
            price2buy = _tab2.PriceBestBid;
            // buy < sell
            spread = price1sell - price2buy;

            if (spread > 0.00001m)
            {
                // buy  price2
                // sell price1
                //_tab1.SellAtLimit(_lotEntrySize, _tab1.PriceBestBid);
                //_tab2.BuyAtLimit(_lotEntrySize, _tab2.PriceBestAsk);


                SendNewLogMessage("_lotEntrySize " + _lotEntrySize
                    + '\n'
                    + " SellAtLimit " + price1sell
                    + '\n'
                    + " _tab2.SellAtLimit " + price2buy
                    + '\n'
                    + " spread " + spread, LogMessageType.NoName
                    );


            }




        }



        #endregion end TradeLogic ==============================




        #region Methods ==============================

        private void _tab1_BestBidAskChangeEvent(decimal arg1, decimal arg2)
        {
            // если не разрешено торговать то возврат
            if (Regime.ValueString == "Off") { return; }
            // вкладки не готовы 
            if (_tab1.IsReadyToTrade == false || _tab2.IsReadyToTrade == false)
            { return; }


            Trade();
        }


        private void _tab2_BestBidAskChangeEvent(decimal arg1, decimal arg2)
        {
            // если не разрешено торговать то возврат
            if (Regime.ValueString == "Off") { return; }
            // вкладки не готовы 
            if (_tab1.IsReadyToTrade == false || _tab2.IsReadyToTrade == false)
            { return; }


            Trade();
        }

        //private void _tabIndex_SpreadChangeEvent(List<Candle> candles)
        //{
        //    // если не разрешено торговать то возврат
        //    if (Regime.ValueString == "Off") { return; }
        //    // вкладки не готовы 
        //    if (_tab1.IsReadyToTrade == false || _tab2.IsReadyToTrade == false)
        //    { return; }


        //    Trade(candles);

        //}

        /// <summary>
        /// пересчитывает значение с вкладки BotTabSimple _tabSimp из долларов decimal baks в необходимое количество монет 
        /// </summary>
        /// <param name="_tabSimp"> вкладка </param>
        private decimal СalculationVolumePosition(BotTabSimple _tabSimp, decimal baks)
        {
            if (_tabSimp.StartProgram == StartProgram.IsTester)
            {
                return Rounding(baks / _tabSimp.PriceCenterMarketDepth, _tabSimp.Securiti.DecimalsVolume);
            }
            if (_tabSimp.IsConnected && _tabSimp.StartProgram == StartProgram.IsOsTrader)
            {
                return Rounding(baks / _tabSimp.PriceCenterMarketDepth, _tabSimp.Securiti.DecimalsVolume);

            }
            else return 0;
        }

        /// <summary>
        /// округляет decimal value до int numbers чисел после запятой
        /// </summary>
        public decimal Rounding(decimal value, int numbers)
        {
            return decimal.Round(value, numbers, MidpointRounding.ToEven);
            //return chah;
        }

        /// <summary>
        /// вывод в дебаг текста 
        /// </summary>
        public static void PrinTextDebug(string text, string secondLine = "")
        {
            string Time = DateTime.Now.ToString("hh:mm:ss:ffff");
            string str = Time + " \n "
                + text + " \n "
                + secondLine + " " + "\n";
            Debug.WriteLine(str);
        }

        public override string GetNameStrategyType()
        {
            return nameof(InterstockArbitrage);
        }

        public override void ShowIndividualSettingsDialog()
        {
            return;
        }

        #endregion end Methods ==============================



        #region Fields ==============================
        /// <summary>
        /// вкладка с первым инструметом
        /// </summary>
        private BotTabSimple _tab1;
        /// <summary>
        /// вкладка со вторым инструментом
        /// </summary>
        private BotTabSimple _tab2;
        /// <summary>
        /// вкладка для индекса
        /// </summary>
        private BotTabIndex _tabIndex;

        /// <summary>
        /// режим торгуем - не торгуем
        /// </summary>
        private StrategyParameterString Regime;

        /// <summary>
        /// объем первой бумаги
        /// </summary>
        private StrategyParameterDecimal VolumeInDollars;





        #endregion end Fields ==============================







    }
}