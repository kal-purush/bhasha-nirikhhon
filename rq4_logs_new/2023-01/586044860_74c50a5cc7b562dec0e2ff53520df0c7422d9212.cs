using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Windows;
using Application.Interfaces;
using Application.Services;
using CommonServiceLocator;
using Engine.Interfaces;
using Engine.Interfaces.Config;
using Engine.Models.Config;
using Engine.Services;
using Kgb.ChessApp.Views;
using Newtonsoft.Json;
using Prism.Ioc;
using Prism.Mvvm;
using Prism.Regions;
using Prism.Unity;

namespace Kgb.ChessApp
{
    /// <summary>
    /// Interaction logic for App.xaml
    /// </summary>
    public partial class App : PrismApplication
    {
        protected override void RegisterTypes(IContainerRegistry containerRegistry)
        {
            var s = File.ReadAllText(@"Config\Configuration.json");
            Configuration configuration = JsonConvert.DeserializeObject<Configuration>(s);

            var x = File.ReadAllText(@"Config\StaticTables.json");
            StaticTableCollection collection = JsonConvert.DeserializeObject<StaticTableCollection>(x);

            var z = File.ReadAllText(@"Config\Table.json");
            Dictionary<int, TableConfiguration> table = JsonConvert.DeserializeObject<Dictionary<int, TableConfiguration>>(z);

            Evaluation evaluation = configuration.Evaluation;
            IConfigurationProvider configurationProvider = new ConfigurationProvider(configuration.AlgorithmConfiguration, new EvaluationProvider(evaluation.Static, evaluation.Opening, evaluation.Middle, evaluation.End),
                configuration.GeneralConfiguration, configuration.PieceOrderConfiguration);
            containerRegistry.RegisterInstance(configurationProvider);

            IStaticValueProvider staticValueProvider = new StaticValueProvider(collection);
            containerRegistry.RegisterInstance(staticValueProvider);

            ITableConfigurationProvider tableConfigurationProvider = new TableConfigurationProvider(table, configurationProvider);
            containerRegistry.RegisterInstance(tableConfigurationProvider);

            containerRegistry.RegisterSingleton(typeof(IMoveProvider), typeof(MoveProvider));
            containerRegistry.RegisterSingleton(typeof(IMoveSorterProvider), typeof(MoveSorterProvider));
            containerRegistry.RegisterSingleton(typeof(IMoveFormatter), typeof(MoveFormatter));
            containerRegistry.RegisterSingleton(typeof(IMoveHistoryService), typeof(MoveHistoryService));
            containerRegistry.RegisterSingleton(typeof(IEvaluationService), typeof(EvaluationService));
            containerRegistry.RegisterSingleton(typeof(ICheckService), typeof(CheckService));
            containerRegistry.RegisterSingleton(typeof(IKillerMoveCollectionFactory), typeof(KillerMoveCollectionFactory));
            containerRegistry.RegisterSingleton(typeof(IAttackEvaluationService), typeof(AttackEvaluationService));
            containerRegistry.RegisterSingleton(typeof(IOpeningService), typeof(OpeningService));
            containerRegistry.RegisterSingleton(typeof(IProbCutModelProvider), typeof(ProbCutModelProvider));
            containerRegistry.RegisterSingleton(typeof(ITranspositionTableService), typeof(TranspositionTableService));


            containerRegistry.RegisterSingleton(typeof(IStrategyProvider), typeof(StrategyProvider));

            containerRegistry.RegisterSingleton(typeof(StartViewModel));
            containerRegistry.RegisterSingleton(typeof(GameViewModel));
        }

        protected override Window CreateShell()
        {
            var regionManager = ServiceLocator.Current.GetInstance<IRegionManager>();
            regionManager.RegisterViewWithRegion("Main", typeof(StartView));
            regionManager.RegisterViewWithRegion("Main", typeof(GameView));

            return new Shell();
        }

        #region Overrides of PrismApplicationBase

        protected override void  ConfigureViewModelLocator()
        {
            ViewModelLocationProvider.SetDefaultViewTypeToViewModelTypeResolver(viewType =>
            {
                var viewName = viewType.FullName;
                var viewAssemblyName = viewType.Assembly.FullName;
                var viewModelName = string.Format(CultureInfo.InvariantCulture, "{0}Model, {1}", viewName, viewAssemblyName);
                return Type.GetType(viewModelName);
            });

            ViewModelLocationProvider.SetDefaultViewModelFactory(vmType =>
            {
                var resolve = ServiceLocator.Current.GetInstance(vmType);
                return resolve;
            });
        }

        #endregion
    }
}