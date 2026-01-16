using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Drawing;
using System.Linq;
using System.Windows;
using System.Windows.Documents;
using System.Windows.Forms.DataVisualization.Charting;
using OsEngine.Charts.CandleChart.Indicators;
using OsEngine.Entity;
using OsEngine.Indicators;
using OsEngine.Language;
using OsEngine.Market;
using OsEngine.OsTrader.Panels;
using OsEngine.OsTrader.Panels.Attributes;
using OsEngine.OsTrader.Panels.Tab;
using static System.Windows.Forms.VisualStyles.VisualStyleElement;


[Bot("TwoLegsBot")]
public class TwoLegsBot : BotPanel
    {

        #region ================поля=(параметров)======================

        private BotTabSimple _tabSimp1;
        private BotTabSimple _tabSimp2;
        private BotTabIndex _tabIndex;

        /// <summary>
        /// вкл.выкл робота
        /// </summary>
        public StrategyParameterBool IsOn;
        /// <summary>
        /// делать одну сделку 
        /// </summary>
        public StrategyParameterBool OneDeal;
        /// <summary>
        /// режим расчета объема позиций
        /// </summary>
        public StrategyParameterString RegimeCalc;
        /// <summary>
        /// режим болинджера стопов
        /// </summary>
        public StrategyParameterString RegimeBolin2;
        /// <summary>
        /// количество баксов на вход 
        /// </summary>
        private StrategyParameterInt Bucks;
        private StrategyParameterInt PercentFromDepo;

        /// <summary>
        /// Машка для профита
        /// </summary>
        private Aindicator _maProfit;
        public StrategyParameterInt MaProfitLength;

        /// <summary>
        /// болинжер вход
        /// </summary>
        private Aindicator _bollingerIn;
        public StrategyParameterDecimal BollingerInDeviation;
        public StrategyParameterInt BollingerInLength;

        /// <summary>
        /// болинжер для стопа
        /// </summary>
        private Aindicator _bollingerStop;
        public StrategyParameterDecimal BollingerStopDeviation;
        public StrategyParameterInt BollingerStopLength;

        // поля 
        private decimal _stopUp = 0;
        private decimal _stopDown = 0;   

        ///// <summary>
        ///// рыночная цена бумаги на вкладке 1
        ///// </summary>
        //private decimal _marketPrice1 = 0;

        ///// <summary>
        ///// рыночная цена бумаги на вкладке 2
        ///// </summary>
        //private decimal _marketPrice2 = 0;

        /// <summary>
        /// объем сделки
        /// </summary>
        //private decimal VolumePosition =0;

        /// <summary>
        /// процент квотируемой от депо (баксов)
        /// </summary>
        private decimal _percentbucksDepo = 0;

        #endregion

        #region ================ Свойства  =============================

        /// <summary>
        ///Тикет класс средств в портфеле
        /// </summary>
        public string SecurClass
        {
            get => _securClass;
            set
            {
                _securClass = value;
                //OnPropertyChanged(nameof(SecurClass));
            }
        }
        private string _securClass;

        #endregion

        #region =========== Конструктор ====================================================
        public TwoLegsBot(string name, StartProgram startProgram) : base(name, startProgram)
        {
            //создание вкладки индекса 
            TabCreate(BotTabType.Index);
            _tabIndex = TabsIndex[0];

            // настройки параметров
            IsOn = CreateParameter("Включен", false, "Вход");
            OneDeal = CreateParameter("Только одна сделка", true, "Вход");
            RegimeCalc = CreateParameter("Режим расчета позы", "Percent", new[] { "Percent", "Bucks" }, "Вход");
            PercentFromDepo = CreateParameter("Процентов от депозита", 5, 5, 100, 5, "Вход");
            Bucks = CreateParameter("Бакстов на вход", 10, 10, 200, 10, "Вход");
            RegimeBolin2 = CreateParameter("Режим Стопов", "ВКЛючены", new[] { "ВКЛючены", "ВЫКЛючены" }, "Вход");

            BollingerInLength = CreateParameter("Длина болинджера входа ", 720, 60, 720, 60, "индюки");
            BollingerInDeviation = CreateParameter("Отклонение болинджера входа ", 3, 0.5m, 8, 0.1m, "индюки");
            _bollingerIn = IndicatorsFactory.CreateIndicatorByName("Bollinger", name + "Bollinger", false);
            _bollingerIn = (Aindicator)_tabIndex.CreateCandleIndicator(_bollingerIn, "Prime");
            _bollingerIn.ParametersDigit[0].Value = BollingerInLength.ValueInt;
            _bollingerIn.ParametersDigit[1].Value = BollingerInDeviation.ValueDecimal;

            BollingerStopLength = CreateParameter("Длина болинджера стопа", 720, 60, 720, 60, "индюки");
            BollingerStopDeviation = CreateParameter("Отклонение болинджера стопа", 4.2m, 0.5m, 8, 0.1m, "индюки");
            _bollingerStop = IndicatorsFactory.CreateIndicatorByName("Bollinger", name + "BollingerStop", false);
            _bollingerStop = (Aindicator)_tabIndex.CreateCandleIndicator(_bollingerStop, "Prime");
            _bollingerStop.ParametersDigit[0].Value = BollingerStopLength.ValueInt;
            _bollingerStop.ParametersDigit[1].Value = BollingerStopDeviation.ValueDecimal;

            MaProfitLength = CreateParameter("Длина машки профита", 360, 60, 720, 60, "индюки");
            _maProfit = IndicatorsFactory.CreateIndicatorByName("Sma", name + "Sma", false);
            _maProfit = (Aindicator)_tabIndex.CreateCandleIndicator(_maProfit, "Prime");
            _maProfit.ParametersDigit[0].Value = MaProfitLength.ValueInt;

            //создание вкладки 1 инструмента
            TabCreate(BotTabType.Simple);
            _tabSimp1 = TabsSimple[0];
        

            //создание вкладки 2 инструмента
            TabCreate(BotTabType.Simple);
            _tabSimp2 = TabsSimple[1];
        
            _tabIndex.SpreadChangeEvent += _tabIndex_SpreadChangeEvent;

            _tabSimp1.PositionClosingSuccesEvent += _tabSimp1_PositionClosingSuccesEvent; // для обнуления параметров 
            _tabSimp2.PositionClosingSuccesEvent += _tabSimp2_PositionClosingSuccesEvent; // для обнуления параметров 

            ParametrsChangeByUser += TwoLegsBot_ParametrsChangeByUser;        

        }

        #endregion

        #region ======== Методы ==========================================================

        #region ===========  Логика =========================================================
        /// <summary>
        ///  пришел новый индекс
        /// </summary>
        private void _tabIndex_SpreadChangeEvent(List<Candle> indexCandel)
        {
            IndexToProfit(indexCandel);
            IndexStopLoss(indexCandel);
            IndexToOpen(indexCandel);
        }
 
        /// <summary>
        /// проверка (индекса) на открытие позиции
        /// </summary>
        private void IndexToOpen(List<Candle> indexCandel)
        {
            if (IsOn.ValueBool == true)
            {
                if (IndicatorsTrue(indexCandel))
                {
                    if (secondCandeleExitUpIndexInOpenPos(indexCandel))
                    {
                        decimal bolInUp = _bollingerIn.DataSeries[0].Last;
                        decimal lastCandleLow = indexCandel[indexCandel.Count - 1].Low;

                        if (bolInUp > lastCandleLow)
                        {// логика открытия над болинжером

                            if (!ExistOpenPosition(_tabSimp1) && !ExistOpenPosition(_tabSimp2) || _percentbucksDepo == 0)
                            {
                                Percentbucks(_tabSimp1);
                            }
                            if (secondCandeleExitDownIndexInOpenPos(indexCandel))
                            {
                                decimal vol = СalculationVolumePosition(_tabSimp1);
                                _tabSimp1.SellAtMarket(vol);
                                SaveStop();
                            }
                            PrinTextDebag("Пересекли Верхний боленждер входа ", " открываем");

                            if (secondCandeleExitDownIndexInOpenPos(indexCandel))
                            {
                                decimal vol = СalculationVolumePosition(_tabSimp2);
                                _tabSimp2.BuyAtMarket(vol);
                                SaveStop();
                            }
                        }
                    };
                    if (secondCandeleExitDownIndexInOpenPos(indexCandel))
                    {
                        decimal bolInDown = _bollingerIn.DataSeries[1].Last;
                        decimal lastCandleHigh = indexCandel[indexCandel.Count - 1].High;
                        if (bolInDown < lastCandleHigh)
                        {// логика открытия под болинжером
                            if (!ExistOpenPosition(_tabSimp1) && !ExistOpenPosition(_tabSimp2) || _percentbucksDepo == 0)
                            {
                                Percentbucks(_tabSimp1);
                            }
                            if (!ExistOpenPosition(_tabSimp1))
                            {
                                decimal vol = СalculationVolumePosition(_tabSimp1);
                                _tabSimp1.BuyAtMarket(vol);
                                SaveStop();
                            }
                            PrinTextDebag("Пересекли нижний боленждер входа", " открываем");
                            if (!ExistOpenPosition(_tabSimp2))
                            {
                                decimal vol = СalculationVolumePosition(_tabSimp2);
                                _tabSimp2.SellAtMarket(vol);
                                SaveStop();
                            }
                        }
                    }
                }
            }
        }

        /// <summary>
        /// проверка пересеения индекса ПРОФИТА
        /// </summary>
        private void IndexToProfit(List<Candle> indexCandel)
        {// логика закрытия позиций 
            if (IndicatorsTrue(indexCandel))
            {
                decimal ma = _maProfit.DataSeries[0].Last;
                decimal lastCandleLow = indexCandel[indexCandel.Count - 1].Low;
                decimal lastCandleHigh = indexCandel[indexCandel.Count - 1].High;
                if (lastCandleLow < ma)
                {
                    if (ma < lastCandleHigh)
                    {
                        PrinTextDebag("индекс пересек машку ", "ПРОФИТ закрываем");
                        _tabSimp1.CloseAllAtMarket();
                        _tabSimp2.CloseAllAtMarket();
                    }
                }
            }
        }
        /// <summary>
        /// проверка пересеения индекса СТОП лоса
        /// </summary>
        private void IndexStopLoss(List<Candle> indexCandel)
        {
            if (IndicatorsTrue(indexCandel))
            {
                decimal lastCandleLow = 0;
                lastCandleLow = indexCandel[indexCandel.Count - 1].Low;
                if (lastCandleLow > _stopUp && _stopUp !=0 && lastCandleLow != 0)
                {
                    PrinTextDebag("индекс стопа перешел за верхний уровень ", "стопимся");

                    if (RegimeBolin2.ValueString == "ВКЛючены")
                    {
                        _tabSimp1.CloseAllAtMarket();
                        _tabSimp2.CloseAllAtMarket();
                    }
                }
                decimal CandleHigh = 0;
                CandleHigh = indexCandel[indexCandel.Count - 1].High;
                if (CandleHigh < _stopDown && _stopDown !=0 && CandleHigh != 0)
                {
                    PrinTextDebag("индекс стопа перешел за нижний уровень", "стопимся");
                    if (RegimeBolin2.ValueString == "ВКЛючены")
                    {
                        _tabSimp1.CloseAllAtMarket();
                        _tabSimp2.CloseAllAtMarket();
                    }
                }
            }
        }
        /// <summary>
        /// сохраняем значения индекса для стопов
        /// </summary>
        private void SaveStop()
        {
            _stopUp = GetsecondCandeleExitUpIndexStop();
            _stopDown = GetsecondCandeleExitDownIndexStop();
        }
        /// <summary>
        ///  закрываем позиции
        /// </summary>
        /// <param name="_tabSimp">вкладка</param>
 
        #endregion

        #region =========== Флаги =====================================================

        /// <summary>
        /// есть ли открытые позиций
        /// </summary>
        /// <param name="_tabSimp">Торговая вкладка</param>
        /// <returns></returns>
        private bool ExistOpenPosition(BotTabSimple _tabSimp)
        {
            List<Position> positions = _tabSimp.PositionsOpenAll;
            if (positions.Count == 0 || positions == null )
            {
                return false;
            }
            else return true;
        }
        /// <summary>
        /// проверка готовности индикаторов
        /// </summary>
        private bool IndicatorsTrue(List<Candle> indexCandel)
        {
            if (_bollingerIn.DataSeries[0].Values == null ||
                indexCandel.Count < _bollingerIn.ParametersDigit[0].Value + 2 ||
                indexCandel.Count < _maProfit.ParametersDigit[0].Value)
            {
                return false;
            }
            else return true;
        }
        /// <summary>
        ///  предыдущая свеча индекса выходила за  верхний болинджер входа
        /// </summary>
        private bool secondCandeleExitUpIndexInOpenPos(List<Candle> indexCandel)
        {
            decimal bolInUp = _bollingerIn.DataSeries[0].Last;
            // decimal bolInDown = _bollingerIn.DataSeries[1].Last;
            decimal secondLastCandleLow = indexCandel[indexCandel.Count - 2].Low;
            // decimal secondLastCandleHigh = indexCandel[indexCandel.Count - 2].High;
            if (bolInUp < secondLastCandleLow)
            {
                return true;
            }
            else return false;
        }
        /// <summary>
        ///  предыдущая свеча индекса выходила за  нижний  болинджер входа
        /// </summary>
        private bool secondCandeleExitDownIndexInOpenPos(List<Candle> indexCandel)
        {
            // decimal bolInUp = _bollingerIn.DataSeries[0].Last;
            decimal bolInDown = _bollingerIn.DataSeries[1].Last;
            // decimal secondLastCandleLow = indexCandel[indexCandel.Count - 2].Low;
            decimal secondLastCandleHigh = indexCandel[indexCandel.Count - 2].High;
            if (bolInDown > secondLastCandleHigh)
            {
                return true;
            }
            else return false;
        }

        /// <summary>
        /// запрос значения верхнего болинджера2 для стопа
        /// </summary>
        private decimal GetsecondCandeleExitUpIndexStop()
        {
            decimal bolInUp = _bollingerStop.DataSeries[0].Last;
            {
                return bolInUp;
            }
        }
        /// <summary>
        /// запрос значения нижнего болинджера2 для стопа
        /// </summary>
        private decimal GetsecondCandeleExitDownIndexStop()
        {
            decimal bolInDown = _bollingerStop.DataSeries[1].Last;
            {
                return bolInDown;
            }
        }

        #endregion

        #region ========= сервисный ===================================================

        private void getCandle()
    {
        // List<Candle> candles =  GetCandleHistory(series.Security.Name,
        //series.TimeFrameSpan);
    }

    /// <summary>
    /// Юзер изменил натройки
    /// </summary>
        private void TwoLegsBot_ParametrsChangeByUser()
        {
            if (_bollingerIn.ParametersDigit[0].Value != BollingerInLength.ValueInt
                || _bollingerIn.ParametersDigit[1].Value != BollingerInDeviation.ValueDecimal)
            {
                _bollingerIn.ParametersDigit[0].Value = BollingerInLength.ValueInt;
                _bollingerIn.ParametersDigit[1].Value = BollingerInDeviation.ValueDecimal;
                _bollingerIn.Save();
                _bollingerIn.Reload();
            }
            if (_bollingerStop.ParametersDigit[0].Value != BollingerStopLength.ValueInt
                || _bollingerStop.ParametersDigit[1].Value != BollingerStopDeviation.ValueDecimal)
            {
                _bollingerStop.ParametersDigit[0].Value = BollingerStopLength.ValueInt;
                _bollingerStop.ParametersDigit[1].Value = BollingerStopDeviation.ValueDecimal;
                _bollingerStop.Save();  
                _bollingerStop.Reload();    
            }
            if (_maProfit.ParametersDigit[0].Value != MaProfitLength.ValueInt)
            {
                _maProfit.ParametersDigit[0].Value = MaProfitLength.ValueInt;
                _maProfit.Save();   
                _maProfit.Reload(); 
            }
        }

        private void _tabSimp2_PositionClosingSuccesEvent(Position position)
        {
            ZeroingValues();
        }

        private void _tabSimp1_PositionClosingSuccesEvent(Position position)
        {
            ZeroingValues();
        }

        /// <summary>
        /// округляет децимал до n чисел после запятой
        /// </summary>
        public decimal Rounding(decimal vol, int n) // округляет децимал до n чисел после запятой
        {
            decimal value = vol;
            int N = n;
            decimal chah = decimal.Round(value, N, MidpointRounding.ToEven);
            return chah;
        }

        /// <summary>
        ///  запрос децимал бумаги для расчета объема сделки по монете 
        /// </summary>
        private int GetDecimalsVolumeSecur(BotTabSimple _tabSimp)
        {
            if (_tabSimp.IsConnected && _tabSimp.StartProgram == StartProgram.IsOsTrader)
            {
                return _tabSimp.Securiti.DecimalsVolume;
            }
            if (_tabSimp.StartProgram == StartProgram.IsTester)
            {
                return _tabSimp.Securiti.DecimalsVolume;
            }
            else return 0;
        }

        /// <summary>
        /// пересчитывает долоры в необходимое количество монет 
        /// </summary>
        /// <param name="_tabSimp"> вкладка </param>
        private decimal СalculationVolumePosition(BotTabSimple _tabSimp)
        {
            if (_tabSimp1.StartProgram == StartProgram.IsTester)
            {
                int baks = Bucks.ValueInt; //  в тестере берем для расчета значение из парметра "баксов на вход"
                decimal VolumePosCoin = 0;
                int decimalSecur = GetDecimalsVolumeSecur(_tabSimp); // берем децимал монеты
                VolumePosCoin = Rounding(baks / _tabSimp.PriceCenterMarketDepth, decimalSecur); // считаем объем в монетах

                return VolumePosCoin;
            }
            if (_tabSimp.IsConnected && _tabSimp1.StartProgram == StartProgram.IsOsTrader)
            {
                decimal VolumePosCoin = 0;
                int decimalSecur = GetDecimalsVolumeSecur(_tabSimp); // берем децимал монеты

                if (RegimeCalc.ValueString == "Percent")
                {
                    VolumePosCoin = Rounding(_percentbucksDepo / _tabSimp.PriceCenterMarketDepth, decimalSecur); // считаем объем в монетах

                    string str = "VolumePosCoin = " + VolumePosCoin.ToString() + "\n";
                    Debug.WriteLine(str);
                    return VolumePosCoin;

                }
                if (RegimeCalc.ValueString == "Bucks")
                {
                    int baks = Bucks.ValueInt;
                    VolumePosCoin = Rounding(baks / _tabSimp.PriceCenterMarketDepth, decimalSecur); // считаем объем в монетах

                    string str = "VolumePosCoin = " + VolumePosCoin.ToString() + "\n";
                    Debug.WriteLine(str);
                    return VolumePosCoin;
                }
                else return VolumePosCoin;

            }
            else return 0;
        }

        /// <summary>
        /// расчитывает количество в процентах от свободных в портфеле в квотиремой (баксов)
        /// </summary>
        private void Percentbucks(BotTabSimple _tabSimp)
        {
            if (_tabSimp1.StartProgram == StartProgram.IsTester)
            {
                _percentbucksDepo = 100;
            }
            if (_tabSimp.IsConnected && _tabSimp1.StartProgram == StartProgram.IsOsTrader)
            {
                decimal depo = 0;
                depo = GetBalans(_tabSimp); // берем свободные на счету
                if (depo != 0)
                {
                    decimal percentbucks = depo / 100 * PercentFromDepo.ValueInt; // высисляем процент от депо
                    _percentbucksDepo = Rounding(percentbucks, 0); // считаем объем в монетах
                    string str = " Баксов в процентах от депо = " + _percentbucksDepo.ToString() + "\n";
                    Debug.WriteLine(str);
                }
            }
        }

        /// <summary>
        /// взять баланс квотируемой валюты
        /// </summary>
        private decimal GetBalans(BotTabSimple _tabSimp)
        {
            if (_securClass != null && _tabSimp.IsConnected && _tabSimp.StartProgram == StartProgram.IsOsTrader)
            {
                decimal balans = _tabSimp.Portfolio.GetPositionOnBoard().Find(pos =>
                pos.SecurityNameCode == _securClass).ValueCurrent;
                return balans;
            }
            return 0;
        }
        public override string GetNameStrategyType()
        {
            return "TwoLegsBot";
        }

        public override void ShowIndividualSettingsDialog()
        {

        }

        /// <summary>
        /// вывод в дебаг текста 
        /// </summary>
        public static void PrinTextDebag(string text, string secondLine = "")
        {
            string Time = DateTime.Now.ToString("hh:mm:ss:ffff");
            string str = text + " \n "
                    + secondLine + " " + Time + "\n";
            Debug.WriteLine(str);
        }
        /// <summary>
        /// обнуляет значения полей 
        /// </summary>
        private void ZeroingValues()
        {
            _percentbucksDepo = 0;
               _stopUp = 0;
             _stopDown = 0;
            if (OneDeal.ValueBool == true)
            {
                IsOn.ValueBool = false; // выключаем робот после сделки 
            }
        }
        #endregion

        #region ====== заготовки==========================
        private bool CandeleExitUpIndexStop(List<Candle> indexCandel)
        {
            decimal bolInUp = _bollingerStop.DataSeries[0].Last;
            // decimal bolInDown = _bollingerIn.DataSeries[1].Last;
            decimal secondLastCandleLow = indexCandel[indexCandel.Count - 1].Low;
            // decimal secondLastCandleHigh = indexCandel[indexCandel.Count - 2].High;
            if (bolInUp < secondLastCandleLow)
            {
                return true;
            }
            else return false;
        }

        private void ClosePositions(BotTabSimple _tabSimp)
        {
            if (ExistOpenPosition(_tabSimp))
            {
                // закрываем на разных вкладках сделки
                PrinTextDebag("индекс перешол за стоп ");
                _tabSimp1.CloseAllAtMarket();
                _tabSimp2.CloseAllAtMarket();
            }

        }
        /// <summary>
        ///  открываем позиции
        /// </summary>
        /// <param name="_tabSimp">вкладка</param>
        private void OpenPosition(BotTabSimple _tabSimp)
        {
            if (!ExistOpenPosition(_tabSimp))
            {
                // открываем на разных вкладках сделки на одинаковый объем в разых направлениях

            };
        }

        /// <summary>
        ///  предыдущая свеча индекса выходила за нижний уровень Стопа 
        /// </summary>
        private bool CandeleExitDownIndexStop(List<Candle> indexCandel)
        {
            // decimal bolInUp = _bollingerIn.DataSeries[0].Last;
            decimal bolInDown = _bollingerStop.DataSeries[1].Last;
            // decimal secondLastCandleLow = indexCandel[indexCandel.Count - 2].Low;
            decimal LastCandleHigh = indexCandel[indexCandel.Count - 1].High;
            if (bolInDown > LastCandleHigh)
            {
                return true;
            }
            else return false;
        }

        #endregion

        #endregion
    }
