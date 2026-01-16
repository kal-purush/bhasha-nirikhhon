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

namespace OsEngine.OsaExtension.MyBots.meshOnSMA

{
    [Bot("meshOnSMA")]

    internal class meshOnSMA : BotPanel
    {
        public meshOnSMA(string name, StartProgram startProgram) : base(name, startProgram)
        {
            this.TabCreate(BotTabType.Simple);
            _tab = TabsSimple[0];

            Mode = this.CreateParameter("Mode", "Edit", new[] { "Edit", "Trade" });
            _smaLenght = CreateParameter("_smaLenght  ", 100, 1, 1000, 10);
            // добавим сма на график 
            _sma = new MovingAverage(name + "Sma", false);
            _sma = (MovingAverage)_tab.CreateCandleIndicator(_sma, "Prime");
            _sma.Lenght = _smaLenght.ValueInt;

            // на график линию средней входа для всех поз
            averegeEntryPriceLevelLine = new LineHorisontal("averegeEntryPriceLevelLine", "Prime", false)
            {
                Color = Color.Yellow,
                Value = 0
            };
            _tab.SetChartElement(averegeEntryPriceLevelLine);
            averegeEntryPriceLevelLine.Value = 0;
            averegeEntryPriceLevelLine.TimeEnd = DateTime.Now;

            // событие закрылась свечка
            _tab.CandleFinishedEvent += _tab_CandleFinishedEvent;


        }


        #region Fields ==============================
        private BotTabSimple _tab;
        private StrategyParameterString Mode;
        private MovingAverage _sma;
        /// <summary>
        /// длинна МАшки
        /// </summary>
        private StrategyParameterInt _smaLenght;
        /// <summary>
        /// линия средней цены входа
        /// </summary>
        public LineHorisontal averegeEntryPriceLevelLine;
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


        #endregion end Fields ==============================


        #region Methods ==============================
        // свеча закрылась
        private void _tab_CandleFinishedEvent(List<Candle> candles)
        {

            // ждем накопления свечек   для корректного расчета cма
            if (candles.Count < _sma.Lenght + 5) { return; }

            lastCandleClose = candles[candles.Count - 1].Close;
            lastSma = _sma.Values[_sma.Values.Count - 1];

            // если закрытие меньшен чем сма  то открываем  первую позицию
            // если нет позиций делаем первый шаг серии позиций
            if (lastCandleClose < lastSma
                && _tab.PositionOpenLong.Count == 0)
            { OpenNewMesh(); }


        }

        private void OpenNewMesh()
        {
            // тут откроем нашу сетку с началом от текущей цены

            // шаг сетки 

            // количество уровней

            // лотность на 1 уровень




        }

        public override string GetNameStrategyType()
        {
            return nameof(meshOnSMA);
        }

        public override void ShowIndividualSettingsDialog()
        {
            throw new NotImplementedException();
        }
        #endregion end Methods ==============================



    }
}