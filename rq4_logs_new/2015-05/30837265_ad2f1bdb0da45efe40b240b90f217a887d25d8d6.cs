using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows.Forms;

namespace GoF.CasinoCraps.UserInterface
{
    public partial class MainForm : Form
    {
        private readonly Game game;
        private readonly Player player;

        public MainForm()
        {
            InitializeComponent();

            LoadBets();

            game = new Game();
            player = new Player(5000);

            player.JoinGame(game);
        }

        private void placeBetButton_Click(object sender, EventArgs e)
        {
            string betName = betComboBox.SelectedItem.ToString();

            var bets = GetBets();

            var betType = (from b in bets
                       where b.Name.Equals(betName)
                       select b.GetType()).First();

            var betToPlace = ((Bet)Activator.CreateInstance(betType, Convert.ToInt32(betAmountUpDown.Value)));

            try
            {
                player.PlaceBet(betToPlace);
            }
            catch (CrapsException ex)
            {
                MessageBox.Show(ex.Message);
            }

            RefreshActiveBets();
            RefreshPlayerMoney();
        }

        private void rollDiceButton_Click(object sender, EventArgs e)
        {
            try
            {
                Roll roll = player.RollDice();

                SetDieImage(firstDiePictureBox, roll.FirstDie);
                SetDieImage(secondDiePictureBox, roll.SecondDie);

                RefreshActiveBets();
                RefreshCompletedBets();
                RefreshPlayerMoney();
                RefreshRoundInfo();
            }
            catch (CrapsException ex)
            {
                MessageBox.Show(ex.Message);
            }
        }

        private void SetDieImage(PictureBox pictureBox, int dieValue)
        {
            switch (dieValue)
            {
                case 1:
                    pictureBox.Image = Properties.Resources.die_01;
                    break;
                case 2:
                    pictureBox.Image = Properties.Resources.die_02;
                    break;
                case 3:
                    pictureBox.Image = Properties.Resources.die_03;
                    break;
                case 4:
                    pictureBox.Image = Properties.Resources.die_04;
                    break;
                case 5:
                    pictureBox.Image = Properties.Resources.die_05;
                    break;
                case 6:
                    pictureBox.Image = Properties.Resources.die_06;
                    break;
                default:
                    pictureBox.Image = null;
                    break;
            }
        }

        private void RefreshActiveBets()
        {
            activeBetsListBox.Items.Clear();
            activeBetsListBox.Items.AddRange(game.ActiveBets.ToArray());
        }

        private void RefreshCompletedBets()
        {
            completedBetsListBox.Items.Clear();
            completedBetsListBox.Items.AddRange(game.CompletedBets.ToArray());
        }

        private void RefreshPlayerMoney()
        {
            currentMoneyLabel.Text = string.Format("Current Money: ${0}", player.Money);
        }

        private void RefreshRoundInfo()
        {
            roundInfoLabel.Text = game.GetRoundDescription();
        }

        private void LoadBets()
        {
            var bets = GetBets();

            var betNames = from b in bets
                           select b.Name;

            betComboBox.Items.AddRange(betNames.ToArray());
            betComboBox.SelectedIndex = 0;
        }

        private static List<Bet> GetBets()
        {
            var listOfBets = (from domainAssembly in AppDomain.CurrentDomain.GetAssemblies()
                                from assemblyType in domainAssembly.GetTypes()
                                where typeof(Bet).IsAssignableFrom(assemblyType)
                                select assemblyType).ToList();

            listOfBets.Remove(typeof(Bet));

            List<Bet> bets = listOfBets.ConvertAll<Bet>(t => ((Bet)Activator.CreateInstance(t, 20)));
            return bets;
        }
    }
}