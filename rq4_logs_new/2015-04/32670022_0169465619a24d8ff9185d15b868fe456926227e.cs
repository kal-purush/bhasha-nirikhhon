using System;
using System.Windows;
using System.Windows.Input;
using TowerDefense.Interfaces;
using TowerDefense.WPFCustomControls;

namespace TowerDefense.Main.Commands
{
    public class PlaceTowerCommand : ICommand
    {
        private bool pressed;
        private Type towerType;
        private MouseButtonEventHandler action; 

        public PlaceTowerCommand()
        {
            this.pressed = false;
            this.action = (object sender, MouseButtonEventArgs e) =>
            {
                Application.Current.MainWindow.MouseUp -= action;
                this.pressed = false;
                if (e.Source is WPFCanvas)
                {
                    WPFCanvas canvas = e.Source as WPFCanvas;
                    System.Windows.Point clickedPosition = e.GetPosition(canvas);
                    var towerConstructor = towerType.GetConstructor(new Type[] { typeof(Interfaces.Point) });
                    ITower tower = (ITower)towerConstructor.Invoke(new object[] { new Interfaces.Point(clickedPosition.X, clickedPosition.Y) });
                    ApplicationContext.Instance.Engine.AddGameObject(tower);
                }
            };
        }

        public bool CanExecute(object parameter)
        {
            return true;
        }

        public event EventHandler CanExecuteChanged;

        public void Execute(object parameter)
        {
            this.pressed = true;
            this.towerType = parameter as Type;
            Application.Current.MainWindow.MouseUp += action;
        }
    }
}