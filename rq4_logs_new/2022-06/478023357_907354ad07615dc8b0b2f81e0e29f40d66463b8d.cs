using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Shapes;
using Torpedo.Model;

namespace Torpedo
{
    /// <summary>
    /// Interaction logic for PlaceShip.xaml
    /// </summary>
    public partial class PlaceShip : Window
    {
        private int calculatedCell = -1;
        private string selectedShip = null;
        private char selectedShipUnit;
        private int rows = 10;
        private int columns = 10;
        private bool shipHorizontal = false;
        private static bool isopen = false;
        private static bool currentPlayersTurn = true;

        private char[,] battleshipPlayfield = new char[10, 10];

        private bool vsComputer;
        private bool player1PlaceShips;
        private bool player2PlaceShips;
        private string player1Name;
        private string player2Name;
        private char[,] player1BattleshipPlayfield = new char[10, 10];
        private Grid player1PlayfieldGrid;

        public PlaceShip()
        {
            InitializeComponent(); 
        }
        public PlaceShip(string player1Name)
        {
            InitializeComponent();
            vsComputer = true;
            this.player1Name = player1Name;

            this.Title = player1Name + "'s ship placement";
            welcomeLabel.Content = player1Name + "'s ship placement";
        }
        public PlaceShip(string player1Name, string player2Name)
        {
            InitializeComponent();

            vsComputer = false;
            this.player1Name = player1Name;
            this.player2Name = player2Name;

            this.Title = player1Name + "'s ship placement";
            welcomeLabel.Content = player1Name + "'s ship placement";
        }
        public PlaceShip(string player1Name, string player2Name, Grid playfield, char[,] battleshipPlayfield)
        {
            InitializeComponent();

            player2PlaceShips = true;
            this.player1Name = player1Name;
            this.player2Name = player2Name;
            this.player1BattleshipPlayfield = battleshipPlayfield;
            this.player1PlayfieldGrid = playfield;

            this.Title = player2Name + "'s ship placement";
            welcomeLabel.Content = player2Name + "'s ship placement";

        }

        private Rectangle DefineShips()
        {
            Rectangle ship = new Rectangle()
            {
                Fill = Brushes.BlueViolet
            };
            Position position = new Position(userBattleField.Width / rows, userBattleField.Height / columns);
            ship.Width = position.X;
            ship.Height = position.Y;
            ship.Name = selectedShip;
            return ship;

        }
        private void shipBtn(object sender, RoutedEventArgs e) //select ship type
        {
            var ShipButton = (Button)sender;
            selectedShip = ShipButton.Content.ToString();

            switch (selectedShip)
            {
                case "Carrier":
                    selectedShipUnit = '5';
                    break;
                case "Battleship":
                    selectedShipUnit = '4';
                    break;
                case "Cruiser":
                    selectedShipUnit = '3';
                    break;
                case "Submarine":
                    selectedShipUnit = '2';
                    break;
                case "Destroyer":
                    selectedShipUnit = '1';
                    break;
            }
        }
        private void shipSetName(Rectangle ship, int shipLength)
        {
            switch (shipLength)
            {
                case 5:
                    ship.Name = "Carrier";
                    break;
                case 4:
                    ship.Name = "Battleship";
                    break;
                case 3:
                    ship.Name = "Cruiser";
                    break;
                case 2:
                    ship.Name = "Submarine";
                    break;
                case 1:
                    ship.Name = "Destroyer";
                    break;
            }
        }
        private int CalculateShipLength()
        {
            int length = selectedShip switch
            {
                "Carrier" => 5,
                "Battleship" => 4,
                "Cruiser" => 3,
                "Submarine" => 2,
                "Destroyer" => 1,
                _ => 0,
            };
            return length;
        }
        private int calculateCell() //which cell the cursor is on
        {
            var point = Mouse.GetPosition(userBattleField);

            int row = 0;
            int col = 0;
            double accumulatedHeight = 0.0;
            double accumulatedWidth = 0.0;

            foreach (var rowDefinition in userBattleField.RowDefinitions)
            {
                accumulatedHeight += rowDefinition.ActualHeight;
                if (accumulatedHeight >= point.Y)
                    break;
                row++;
            }

            foreach (var columnDefinition in userBattleField.ColumnDefinitions)
            {
                accumulatedWidth += columnDefinition.ActualWidth;
                if (accumulatedWidth >= point.X)
                    break;
                col++;
            }

            return (row * 10) + col;
        }

        private void onGridMouseClick(object sender, MouseButtonEventArgs e) //ship placement in the playfield
        {
            if (e.ClickCount == 1)
            {
                int shipLength = CalculateShipLength();
                bool shipPlacementEnoughSpace = true;

                for (int i = 0; i < shipLength; i++)
                {
                    int cell = calculateCell();

                    Rectangle ship = DefineShips();

                    //enough space for the selected ship or not
                    shipPlacementEnoughSpace = !shipExtendsBeyond(cell, shipLength, shipHorizontal);

                    //collision with another ship
                    if (shipPlacementEnoughSpace)
                    {
                        shipPlacementEnoughSpace = !shipCollision(i, cell, shipLength, shipHorizontal);
                    }

                    if (shipPlacementEnoughSpace)
                    {
                        shipPlaceToPlayfield(ship, i, cell, shipHorizontal);
                    }
                    else
                    {
                        break;
                    }
                }

                if (shipPlacementEnoughSpace)
                {
                    setSelectedShipButtonDisabled();
                }
            }
        }

        private bool shipCollision(int i, int cell, int shipLength, bool shipHorizontal)
        {
            for (int unit = 0 + i; unit < shipLength; unit++)
            {
                if (shipHorizontal)
                {
                    if (char.IsDigit(battleshipPlayfield[cell / rows, cell % columns + unit]))
                    {
                        return true;
                    }
                }
                else if (!shipHorizontal)
                {
                    if (char.IsDigit(battleshipPlayfield[cell / rows + unit, cell % columns]))
                    {
                        return true;
                    }
                }
            }

            return false;
        }

        private bool shipExtendsBeyond(int cell, int shipLength, bool shipHorizontal)
        {
            if (shipHorizontal)
            {
                if (cell / rows < rows && cell % columns + shipLength - 1 < columns)
                {
                    return false;
                }
            }
            else if (!shipHorizontal)
            {
                if (cell / rows + shipLength - 1 < rows && cell % columns < columns)
                {
                    return false;
                }
            }

            return true;
        }

        private bool everyShipPlaced()
        {
            if (!carrierBtn.IsEnabled && !battleshipBtn.IsEnabled && !cruiserBtn.IsEnabled && !submarineBtn.IsEnabled && !destroyerBtn.IsEnabled)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        private void shipPlaceToPlayfield(Rectangle ship, int i, int cell, bool shipHorizontal)
        {
            if (shipHorizontal)
            {
                Grid.SetRow(ship, cell / rows);
                Grid.SetColumn(ship, cell % columns + i);

                //save the ship position
                battleshipPlayfield[cell / rows, cell % columns + i] = selectedShipUnit;
            }
            else if (!shipHorizontal)
            {
                Grid.SetRow(ship, cell / rows + i);
                Grid.SetColumn(ship, cell % columns);

                //save the ship position
                battleshipPlayfield[cell / rows + i, cell % columns] = selectedShipUnit;
            }

            userBattleField.Children.Add(ship);
        }

        private void setSelectedShipButtonDisabled()
        {
            switch (selectedShip) //placed ship button set disabled
            {
                case "Carrier":
                    carrierBtn.IsEnabled = false;
                    break;
                case "Battleship":
                    battleshipBtn.IsEnabled = false;
                    break;
                case "Cruiser":
                    cruiserBtn.IsEnabled = false;
                    break;
                case "Submarine":
                    submarineBtn.IsEnabled = false;
                    break;
                case "Destroyer":
                    destroyerBtn.IsEnabled = false;
                    break;
            }

            selectedShip = null;
        }
        private void resetBtn_Click(object sender, RoutedEventArgs e)
        {
            selectedShip = null;
            calculatedCell = -1;

            carrierBtn.IsEnabled = true;
            battleshipBtn.IsEnabled = true;
            cruiserBtn.IsEnabled = true;
            submarineBtn.IsEnabled = true;
            destroyerBtn.IsEnabled = true;

            userBattleField.Children.Clear();

            for (int row = 0; row < rows; row++)
            {
                for (int col = 0; col < columns; col++)
                {
                    battleshipPlayfield[row, col] = '\0';
                }
            }
        }

        private void RotateShip()
        {
            shipHorizontal = !shipHorizontal;
        }
        private void ApplyInputKey(Key pressedKey)
        {
            if (pressedKey == Key.Space)
            {
                RotateShip();
            }
        }
        private void Window_KeyDown(object sender, KeyEventArgs e)
        {
            ApplyInputKey(e.Key);
        }
        private void ExitFromGameButton_Click(object sender, RoutedEventArgs e)
        {
            System.Environment.Exit(0);

        }

        private void OkButton_Click(object sender, RoutedEventArgs e)
        {
            //TODO
        }
    }
}