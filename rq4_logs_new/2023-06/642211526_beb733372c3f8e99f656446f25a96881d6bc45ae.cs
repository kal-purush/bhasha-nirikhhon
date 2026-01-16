using OsEngine.Charts.CandleChart.Elements;
using OsEngine.Charts.CandleChart.Indicators;
using OsEngine.Entity;
using OsEngine.Indicators;
using OsEngine.OsTrader.Panels;
using OsEngine.OsTrader.Panels.Attributes;
using OsEngine.OsTrader.Panels.Tab;
using System;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using static System.Windows.Forms.VisualStyles.VisualStyleElement;

namespace OsEngine.OsaExtension.Robots
{
    [Bot("PairTrading")]

    internal class PairTrading : BotPanel
    {

        public PairTrading(string name, StartProgram startProgram) : base(name, startProgram)
        {
            #region  подготовка вкладки ===================================

            // делаем вкладки
            TabCreate(BotTabType.Simple);
            _tab1 = TabsSimple[0];
            //_tab1.CandleFinishedEvent += _tab1_CandleFinishedEvent;
            TabCreate(BotTabType.Simple);
            _tab2 = TabsSimple[1];
            //_tab2.CandleFinishedEvent += _tab2_CandleFinishedEvent;
            TabCreate(BotTabType.Index);
            _tabIndex = TabsIndex[0];
            _tabIndex.SpreadChangeEvent += _tabIndex_SpreadChangeEvent;
            //_tabIndex.CandleFinishedEvent += _tabIndex_CandleFinishedEvent;

            Regime = CreateParameter("Regime", "Off", new[] { "Off", "On" });
            Volume1 = CreateParameter("Volume 1", 1, 1.0m, 50, 1);
            Volume2 = CreateParameter("Volume 2", 1, 1.0m, 50, 1);

            // добавим боллинжер
            BollingerEntryLength = CreateParameter("BollingerEntry Length", 720, 100, 10000, 20);
            BollingerEntryDeviation = CreateParameter("BollingerEntry Deviation", 3, 0.5m, 5, 0.1m);
            _bollingerEntry = IndicatorsFactory.CreateIndicatorByName("Bollinger", name + "Bollinger", false);
            _bollingerEntry = (Aindicator)_tabIndex.CreateCandleIndicator(_bollingerEntry, "Prime");
            _bollingerEntry.ParametersDigit[0].Value = BollingerEntryLength.ValueInt;
            _bollingerEntry.ParametersDigit[1].Value = BollingerEntryDeviation.ValueDecimal;

            //линия входа в позу
            //entryPositionPriceLevelLine = new LineHorisontal("entryPositionPriceLevelLine", "Prime", false)
            //{ Color = Color.YellowGreen, Value = 0, TimeEnd = DateTime.MaxValue };
            //_tabIndex.SetChartElement(entryPositionPriceLevelLine);


            _lotEntrySize = CreateParameter("_lotEntrySize  ", 1m, 0.00001m, 100m, 0.00001m);


            #endregion конец подготовка вкладки ===================================




        }



        #region TradeLogic ==============================
        private void Trade(List<Candle> candles)
        {
            // значения переменных на сейчас
            bollingerLastPriceUp = _bollingerEntry.DataSeries[0].Last;
            bollingerLastPriceDown = _bollingerEntry.DataSeries[1].Last;
            bollingerLastPriceCenter = _bollingerEntry.DataSeries[2].Last;
            bollingerSpread = bollingerLastPriceUp - bollingerLastPriceDown;
            //currentIndexPrice = _tabIndex.PriceCenterMarketDepth;
            currentIndexPrice = candles[candles.Count - 1].Close;



            // надо ли открывать позы?
            List<Position> pos1 = _tab1.PositionsOpenAll;
            List<Position> pos2 = _tab2.PositionsOpenAll;
            if (pos1 == null && pos1.Count == 0 || pos2 == null && pos2.Count == 0)
            { OpenPairPositions(); }


            // были выше болинжера
            if (currentIndexPrice > bollingerLastPriceUp)
            {
                wereWeUpBollinger = true;
            }
            // были ниже болинжера
            if (currentIndexPrice < bollingerLastPriceDown)
            {
                wereWeDownBollinger = true;
            }

            // пересекли центр боллинжера ? пора закрываться
            if (buyIndex && currentIndexPrice < bollingerLastPriceCenter)
            {
                CloseAllPositions();
            }

            // пересекли центр боллинжера ? пора закрываться
            if (sellIndex && currentIndexPrice > bollingerLastPriceCenter)
            {
                CloseAllPositions();
            }



        }

        private void OpenPairPositions()
        {


            // откроем на возврате за болинжер сверху
            if (currentIndexPrice < bollingerLastPriceUp
                && wereWeUpBollinger)
            {
                _tab1.SellAtMarket(_lotEntrySize.ValueDecimal);
                _tab2.BuyAtMarket(_lotEntrySize.ValueDecimal);

                wereWeUpBollinger = false;
                sellIndex = true;

                // нарисуем линию входа 
                //entryPositionPriceLevelLine.Value = currentIndexPrice;
                //entryPositionPriceLevelLine.Refresh();

            }

            // откроем на возврате за болинжер снизу
            if (currentIndexPrice > bollingerLastPriceDown
                && wereWeDownBollinger)
            {
                _tab1.BuyAtMarket(_lotEntrySize.ValueDecimal);
                _tab2.SellAtMarket(_lotEntrySize.ValueDecimal);
                wereWeDownBollinger = false;
                buyIndex = true;

                // нарисуем линию входа 
                //entryPositionPriceLevelLine.Value = currentIndexPrice;
                //entryPositionPriceLevelLine.Refresh();
            }
        }


        private void CloseAllPositions()
        {
            List<Position> positions1 = _tab1.PositionsOpenAll;
            List<Position> positions2 = _tab2.PositionsOpenAll;

            if (positions1.Count != 0 && positions1[0].State == PositionStateType.Open)
            {
                _tab1.CloseAtMarket(positions1[0], positions1[0].OpenVolume);
            }

            if (positions2.Count != 0 && positions2[0].State == PositionStateType.Open)
            {
                _tab2.CloseAtMarket(positions2[0], positions2[0].OpenVolume);
            }
        }

        #endregion end TradeLogic ==============================


        #region Methods ==============================
        /// <summary>
        ///открылась новая свечка на вкладке индекса
        /// </summary>
        private void _tabIndex_SpreadChangeEvent(List<Candle> candles)
        {
            // если не разрешено торговать то возврат
            if (Regime.ValueString == "Off") { return; }

            // ждем накопления свечек   для корректного расчета болинжера
            if (_bollingerEntry.DataSeries[0].Values == null
                || candles.Count < _bollingerEntry.ParametersDigit[0].Value + 2)
            { return; }

            if (_tab1.IsReadyToTrade == false || _tab2.IsReadyToTrade == false)
            { return; }


            Trade(candles);
        }

        private void _tab1_CandleFinishedEvent(List<Candle> list)
        {
            throw new NotImplementedException();
        }
        private void _tab2_CandleFinishedEvent(List<Candle> list)
        {
            throw new NotImplementedException();
        }

        public override string GetNameStrategyType()
        {
            return nameof(PairTrading);
        }

        public override void ShowIndividualSettingsDialog()
        {


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
        private StrategyParameterDecimal Volume1;
        /// <summary>
        /// объем второй бумаги
        /// </summary>
        private StrategyParameterDecimal Volume2;


        /// <summary>
        /// боллинжер 
        /// </summary>
        private Aindicator _bollingerEntry;
        private StrategyParameterDecimal BollingerEntryDeviation;
        private StrategyParameterInt BollingerEntryLength;

        /// <summary>
        /// линия  цены входа на график 
        /// </summary>
        private LineHorisontal entryPositionPriceLevelLine;

        /// <summary>
        /// цена верхнего болинжера
        /// </summary>
        private decimal bollingerLastPriceUp;
        /// <summary>
        /// цена нижнего болинжера
        /// </summary>
        private decimal bollingerLastPriceDown;
        /// <summary>
        /// цена центра болинжера
        /// </summary>
        private decimal bollingerLastPriceCenter;
        /// <summary>
        /// размах болинжера (от низа до верха, типа волатильность)
        /// </summary>
        private decimal bollingerSpread;

        /// <summary>
        /// текущая цена индекса
        /// </summary>
        private decimal currentIndexPrice;

        /// <summary>
        /// размер лота для входа
        /// </summary>
        private StrategyParameterDecimal _lotEntrySize;

        /// <summary>
        /// признак что были выше верхнего болинжер
        /// </summary>
        bool wereWeUpBollinger = false;
        /// <summary>
        /// признак что были ниже нижнего болинжер
        /// </summary>
        bool wereWeDownBollinger = false;
        /// <summary>
        /// признак что продали индекс
        /// </summary>
        bool sellIndex = false;
        /// <summary>
        /// признак что купили индекс
        /// </summary>
        bool buyIndex = false;




        #endregion end Fields ==============================

    }
}