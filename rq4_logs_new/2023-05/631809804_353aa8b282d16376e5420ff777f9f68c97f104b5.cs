using Borromeo_Filomeno_FinalProject.Properties;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Media;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace Borromeo_Filomeno_FinalProject
{
    public abstract class UVEnemy : Uber_Form_Game
    {
        public string EnemyTag { get; set; }

        public static int Instances { get; set; }

        public PictureBox PbEnemy { get; set; }


        public void IsColliding(UVPlayer player, Uber_Form_Game game)
        {

            if (player.PbPlayer.Bounds.IntersectsWith(PbEnemy.Bounds))
            {
                SoundPlayer explosionSound = new SoundPlayer(Resources.explosionsound);
                explosionSound.Play();
                game.IsGameOver = true;
                MessageBox.Show("Game Over!");
            }

        }


        public abstract void ResetEnemy();


        public static void ResetEnemies()
        {
            Random rnd = new Random();

            foreach (var enemy in Enemies)
            {
                if (enemy is UVEnemySmall)
                {
                    //top 50
                    enemy.PbEnemy.Left = rnd.Next(12, 772);
                    enemy.PbEnemy.Top = rnd.Next(0, 1600) * -1;
                }

                if (enemy is UVEnemyMedium)
                {
                    //top 60
                    enemy.PbEnemy.Left = rnd.Next(12, 740);
                    enemy.PbEnemy.Top = rnd.Next(0, 1200) * -1;
                }

                if (enemy is UVEnemyBig)
                {
                    //top 100
                    enemy.PbEnemy.Left = rnd.Next(12, 752);
                    enemy.PbEnemy.Top = rnd.Next(0, 1000) * -1;
                }

            }


        }

        public static void EnemyMovement(UVPlayer player, Uber_Form_Game game)
        {


            foreach (var enemy in Enemies)
            {
                //changes
                enemy.PbEnemy.Visible = true;
                enemy.IsColliding(player, game);


                if (enemy.PbEnemy.Top > 712) enemy.ResetEnemy();

                if (enemy is UVEnemySmall)
                {
                    enemy.SetEnemySpeed(4);
                    enemy.MoveEnemy();

                }

                if (enemy is UVEnemyBig)
                {
                    enemy.SetEnemySpeed(2);
                    enemy.MoveEnemy();

                }

                if (enemy is UVEnemyMedium)
                {
                    enemy.SetEnemySpeed(3);
                    enemy.MoveEnemy();

                }


            }
        }


        public UVEnemy(PictureBox pbEnemy)
        {

            PbEnemy = pbEnemy;
            Instances++;
        }

        public abstract void SetEnemySpeed(int speed);

        public static void SortEnemies(Control.ControlCollection Controls)
        {
            //Allocates memory so that there's an instance variable so that we can add objects
            Enemies = new List<UVEnemy>();
            PbEnemies = new List<PictureBox>();
            foreach (var enemy in Controls)
            {
                if (enemy is PictureBox)
                {
                    var pbEnemy = (PictureBox)enemy;


                    if ((string)pbEnemy.Tag == "enemyBig")
                    {
                        Enemies.Add(new UVEnemyBig(pbEnemy));

                    }

                    if ((string)pbEnemy.Tag == "enemyMedium")
                    {
                        Enemies.Add(new UVEnemyMedium(pbEnemy));

                    }

                    if ((string)pbEnemy.Tag == "enemySmall")
                    {
                        Enemies.Add(new UVEnemySmall(pbEnemy));

                    }

                    PbEnemies.Add(pbEnemy);

                }

            }
        }



        public abstract void MoveEnemy();




    }
}