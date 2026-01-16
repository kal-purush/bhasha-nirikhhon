using OsEngine.Charts.CandleChart.Elements;
using OsEngine.Charts.CandleChart.Indicators;
using OsEngine.Entity;
using OsEngine.OsTrader.Panels;
using OsEngine.OsTrader.Panels.Attributes;
using OsEngine.OsTrader.Panels.Tab;
using System;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static System.Windows.Forms.VisualStyles.VisualStyleElement;

namespace OsEngine.OsaExtension.Robots.NotWork.meshOnSMA

{
    [Bot("meshOnSMA")]

    internal class meshOnSMA : BotPanel
    {
        public meshOnSMA(string name, StartProgram startProgram) : base(name, startProgram)
        {
            this.TabCreate(BotTabType.Simple);
            _tab = TabsSimple[0];

            Mode = this.CreateParameter("Mode", "Edit", new[] { "Edit", "Trade" });
            _smaLength = CreateParameter("_smaLength  ", 100, 1, 1000, 10);
            // добавим сма на график 
            _sma = new MovingAverage(name + "Sma", false);
            _sma = (MovingAverage)_tab.CreateCandleIndicator(_sma, "Prime");
            _sma.Lenght = _smaLength.ValueInt;

            // на график линию средней входа для всех поз
            averagePriceEntryLevelLine = new LineHorisontal("averageEntryPriceLevelLine", "Prime", false)
            { Color = Color.Yellow, Value = 0, TimeEnd = DateTime.Now };
            _tab.SetChartElement(averagePriceEntryLevelLine);

            // на график линию входа 
            entryPriceLevelLine = new LineHorisontal("entryPriceLevelLine", "Prime", false)
            { Color = Color.Blue, Value = 0, TimeEnd = DateTime.Now };
            _tab.SetChartElement(entryPriceLevelLine);

            // количество уровней сетки
            _numberOfMeshLevels = CreateParameter("_numberOfMeshLevels", 5, 1, 50, 1);

            // шаг между уровнями сетки
            _stepOfMeshLevels = CreateParameter("_stepOfMeshLevels", 100m, 1m, 50000m, 100m);

            _lotEntrySize = CreateParameter("_lotEntrySize", 1m, 1, 100, 1);


            // событие закрылась свечка
            _tab.CandleFinishedEvent += _tab_CandleFinishedEvent;


        }


        #region Methods ==============================
        // свеча закрылась
        private void _tab_CandleFinishedEvent(List<Candle> candles)
        {

            // ждем накопления свечек для корректного расчета cма и коннектор
            if (candles.Count < _sma.Lenght + 6 || !_tab.IsReadyToTrade) { return; }

            lastCandleClose = candles[candles.Count - 1].Close;
            lastSma = _sma.Values[_sma.Values.Count - 1];

            // нужен признак что мы были ниже сма
            // если предыдущие Х свечек были ниже сма
            if (
                   candles[candles.Count - 2].Close < lastSma
                && candles[candles.Count - 3].Close < lastSma
                && candles[candles.Count - 4].Close < lastSma
                && candles[candles.Count - 5].Close < lastSma
                && candles[candles.Count - 6].Close < lastSma

                )
            { wereBelowSma = true; }
            else
            { wereBelowSma = false; }

            // если закрытие больше чем сма итд то открываем  лимитную сетку в покупки
            if (lastCandleClose > lastSma
                && _tab.PositionOpenLong.Count == 0
                && wereBelowSma
                //&& canTrade
                )
            {
                OpenNewMeshBuyLimits();
            }


            if (_tab.PositionOpenLong.Count != 0)
            {
                CalculateAveragePriceAllOpenPosition();
            }

        }

        void OpenNewMeshBuyLimits()
        {
            // тут откроем нашу сетку с началом от текущей цены

            decimal priceOpenCurrentStep = _tab.PriceCenterMarketDepth - _stepOfMeshLevels.ValueDecimal;

            //List<LineHorisontal> entryPriceLevelLineList = new List<LineHorisontal>();

            for (int i = 0; i < _numberOfMeshLevels.ValueInt; i++)
            {
                _tab.BuyAtLimit(_lotEntrySize.ValueDecimal, priceOpenCurrentStep);

                priceOpenCurrentStep -= _stepOfMeshLevels.ValueDecimal;

                entryPriceLevelLine.UniqName = priceOpenCurrentStep.ToString();
                //entryPriceLevelLineList.Add(entryPriceLevelLine);
                entryPriceLevelLine.Value = priceOpenCurrentStep;
                entryPriceLevelLine.TimeEnd = DateTime.Now;
                entryPriceLevelLine.Refresh();

            }

        }

        void Close()
        {
            
        }


        /// <summary>
        /// расчет средней цены по всем открытым позам
        /// </summary>
        void CalculateAveragePriceAllOpenPosition()
        {
            // сумма в деньгах всех позиций
            decimal allMoneyAllPositions = 0;

            List<Position> positions = _tab.PositionsOpenAll;

            decimal lotsAccumulator = 0;

            for (int i = 0; i < _tab.PositionsOpenAll.Count; i++)
            {
                decimal lots = positions[i].Lots;
                decimal entryPrice = positions[i].EntryPrice;
                // значение в деньгах
                allMoneyAllPositions += (lots * entryPrice);
                lotsAccumulator += lots;
            }
            averagePriceOpenAllPosition = allMoneyAllPositions / lotsAccumulator;
            // нарисуем среднюю входа
            averagePriceEntryLevelLine.Value = averagePriceOpenAllPosition;
            averagePriceEntryLevelLine.TimeEnd = DateTime.Now;
            averagePriceEntryLevelLine.Refresh();

        }

        public override string GetNameStrategyType()
        {
            return nameof(meshOnSMA);
        }

        public override void ShowIndividualSettingsDialog()
        {
            return;
        }
        #endregion end Methods ==============================



        #region Fields ==============================

        private BotTabSimple _tab;
        private StrategyParameterString Mode;
        private MovingAverage _sma;
        /// <summary>
        /// длинна МАшки
        /// </summary>
        private StrategyParameterInt _smaLength;
        /// <summary>
        /// размер лота для входа
        /// </summary>
        private StrategyParameterDecimal _lotEntrySize;
        /// <summary>
        /// число уровней сетки
        /// </summary>
        private StrategyParameterInt _numberOfMeshLevels;
        /// <summary>
        /// шаг между уровнями сетки
        /// </summary>
        private StrategyParameterDecimal _stepOfMeshLevels;
        /// <summary>
        /// линия средней цены входа
        /// </summary>
        public LineHorisontal averagePriceEntryLevelLine;
        /// <summary>
        /// линия цены входа
        /// </summary>
        public LineHorisontal takeProfitAllPositionsLevelLine;
        /// <summary>
        /// линия средней цены входа
        /// </summary>
        public LineHorisontal entryPriceLevelLine;
        /// <summary>
        /// средняя цена открытия всех позиций (без убыток)
        /// </summary>
        public decimal averagePriceOpenAllPosition = 0m;
        /// <summary>
        /// line color
        /// цвет линии
        /// </summary>
        public Color Color;
        /// <summary>
        /// цена закрытия последней свечки
        /// </summary>
        public decimal lastCandleClose = 0;
        /// <summary>
        /// цена МАшки на последней свечки
        /// </summary>
        decimal lastSma = 0;
        /// <summary>
        /// признак что были ниже сма
        /// </summary>
        bool wereBelowSma = false;
        /// <summary>
        /// можно ли торговать ??
        /// </summary>
        public bool canTrade = false;
        /// <summary>
        /// текущая цена инструмента
        /// </summary>
        public decimal currentSecurityPrice = 0m;



        #endregion end Fields ==============================


    }
}