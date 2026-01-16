using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Drawing;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace PingPong_Multiplayer_Game
{
    class PingPong
    {
        public Label gameover_lbl, p1_points_label, p2_points_label;
        public Panel playground_pnl;
        public PictureBox racket, racket2, ball, ball2;
        public Timer t1;
        public int speed_left = 3;                                  //speed of the ball
        public int speed_top = 3;
        public int speed_bottom = 3;
        public int speed_left2 = 2;                                  //speed of the ball2
        public int speed_top2 = 2;
        public int p1_points = 0;                                      //score points
        public int p2_points = 0;

        public void ping_pong(Label gameover, Label p1_points_lbl, Label p2_points_lbl, Panel plyground, PictureBox rck, PictureBox rck2, PictureBox bal, PictureBox bal2, Timer tm1)
        {
            this.gameover_lbl = gameover;
            this.p1_points_label = p1_points_lbl;
            this.p2_points_label = p2_points_lbl;
            this.playground_pnl = plyground;
            this.racket = rck;
            this.racket2 = rck2;
            this.ball = bal;
            this.ball2 = bal2;
            this.t1 = tm1;

            tm1.Enabled = true;
            gameover.Visible = false;
        }
        public void single_easy_level()
        {
            racket.Left = Cursor.Position.X - (racket.Width / 2);   //set the center of the racket to the position of cursor
            ball.Left += speed_left;                                //move the ball
            ball.Top += speed_top;

            if (ball.Bottom >= racket.Top && ball.Bottom <= racket.Bottom && ball.Left >= racket.Left && ball.Right <= racket.Right) // racket collusion
            {
                speed_top += 1;
                speed_left += 1;
                speed_top = -speed_top;//change direction
                p1_points += 1;

                p1_points_label.Text = p1_points.ToString();


                Random r = new Random();
                playground_pnl.BackColor = Color.FromArgb(r.Next(150, 255), r.Next(150, 255), r.Next(150, 255));   //geting random RGB color and set it as playground backcolor
            }



            if (ball.Left <= playground_pnl.Left)
            {
                speed_left = -speed_left;
            }

            if (ball.Right >= playground_pnl.Right)
            {
                speed_left = -speed_left;
            }

            if (ball.Top <= playground_pnl.Top)
            {
                speed_top = -speed_top;
            }
            //player lose
            if (ball.Bottom >= playground_pnl.Bottom)
            {
                t1.Enabled = false;           //ball is out -> stop the game
                gameover_lbl.Text = "Game Over \nYou Scored " + p1_points.ToString() + " \n   Esc - Exit";
                gameover_lbl.Visible = true;
            }
        }
        public void single_hard_level()
        {
            racket.Left = Cursor.Position.X - (racket.Width / 2);   //set the center of the racket to the position of cursor
            ball.Left += speed_left;                                //move the ball
            ball.Top += speed_top;

            if (ball.Bottom >= racket.Top && ball.Bottom <= racket.Bottom && ball.Left >= racket.Left && ball.Right <= racket.Right) // racket collusion
            {
                speed_top += 2;
                speed_left += 2;
                speed_top = -speed_top;//change direction
                p1_points += 1;

                p1_points_label.Text = p1_points.ToString();


                Random r = new Random();
                playground_pnl.BackColor = Color.FromArgb(r.Next(150, 255), r.Next(150, 255), r.Next(150, 255));   //geting random RGB color and set it as playground backcolor
            }



            if (ball.Left <= playground_pnl.Left)
            {
                speed_left = -speed_left;
            }

            if (ball.Right >= playground_pnl.Right)
            {
                speed_left = -speed_left;
            }

            if (ball.Top <= playground_pnl.Top)
            {
                speed_top = -speed_top;
            }

            //player lose
            if (ball.Bottom >= playground_pnl.Bottom)
            {
                t1.Enabled = false;           //ball is out -> stop the game
                gameover_lbl.Text = "Game Over \nYou Scored " + p1_points.ToString() + " \n   Esc - Exit";
                gameover_lbl.Visible = true;
            }
        }
        public void single_expert_level()
        {
            racket.Left = Cursor.Position.X - (racket.Width / 2);   //set the center of the racket to the position of cursor
            ball.Left += speed_left;                                //move the ball
            ball2.Left += speed_left2;                                //move the ball2
            ball.Top += speed_top;
            ball2.Top += speed_top2;

            if (ball.Bottom >= racket.Top && ball.Bottom <= racket.Bottom && ball.Left >= racket.Left && ball.Right <= racket.Right) // racket collusion
            {
                speed_top += 2;
                speed_left += 2;
                speed_top = -speed_top;//change direction
                p1_points += 1;

                p1_points_label.Text = p1_points.ToString();


                Random r = new Random();
                playground_pnl.BackColor = Color.FromArgb(r.Next(150, 255), r.Next(150, 255), r.Next(150, 255));   //geting random RGB color and set it as playground backcolor
            }

            if (ball2.Bottom >= racket.Top && ball2.Bottom <= racket.Bottom && ball2.Left >= racket.Left && ball2.Right <= racket.Right) // racket collusion with ball2
            {
                speed_top2 += 2;
                speed_left2 += 2;
                speed_top2 = -speed_top2;//change direction
                p1_points += 1;

                p1_points_label.Text = p1_points.ToString();


                Random r = new Random();
                playground_pnl.BackColor = Color.FromArgb(r.Next(150, 255), r.Next(150, 255), r.Next(150, 255));   //geting random RGB color and set it as playground backcolor
            }



            if (ball.Left <= playground_pnl.Left)
            {
                speed_left = -speed_left;
            }
            if (ball2.Left <= playground_pnl.Left)
            {
                speed_left2 = -speed_left2;
            }

            if (ball.Right >= playground_pnl.Right)
            {
                speed_left = -speed_left;
            }
            if (ball2.Right >= playground_pnl.Right)
            {
                speed_left2 = -speed_left2;
            }

            if (ball.Top <= playground_pnl.Top)
            {
                speed_top = -speed_top;
            }
            if (ball2.Top <= playground_pnl.Top)
            {
                speed_top2 = -speed_top2;
            }

            //player lose
            if (ball.Bottom >= playground_pnl.Bottom || ball2.Bottom >= playground_pnl.Bottom)
            {
                t1.Enabled = false;           //ball is out -> stop the game
                gameover_lbl.Text = "Game Over \nYou Scored " + p1_points.ToString() + " \n   Esc - Exit";
                gameover_lbl.Visible = true;
            }
        }

        //multi easy
        public void multi_easy_level(int racket_left)
        {
            racket.Left = Cursor.Position.X - (racket.Width / 2);   //set the center of the racket to the position of cursor
            racket2.Left = racket_left;
            ball.Left += speed_left;                                //move the ball
            ball.Top += speed_top;

            //ball collusion with racket1
            if (ball.Bottom >= racket.Top && ball.Bottom <= racket.Bottom && ball.Left >= racket.Left && ball.Right <= racket.Right) // racket collusion
            {
                speed_top += 1;
                speed_left += 1;
                speed_top = -speed_top;//change direction
                p1_points += 1;

                p1_points_label.Text = p1_points.ToString();


                Random r = new Random();
                playground_pnl.BackColor = Color.FromArgb(r.Next(150, 255), r.Next(150, 255), r.Next(150, 255));   //geting random RGB color and set it as playground backcolor
            }

            //ball collusion with racket2
            //
            //
            //
            //
            if (ball.Bottom <= racket2.Bottom && ball.Bottom <= racket2.Bottom && ball.Left >= racket2.Left && ball.Right <= racket2.Right) // racket collusion
            {
                speed_top += 1;
                speed_left += 1;
                speed_top = +speed_top;//change direction
                p2_points += 1;

                p2_points_label.Text = p2_points.ToString();


                Random r = new Random();
                playground_pnl.BackColor = Color.FromArgb(r.Next(150, 255), r.Next(150, 255), r.Next(150, 255));   //geting random RGB color and set it as playground backcolor
            }

            if (ball.Left <= playground_pnl.Left)
            {
                speed_left = -speed_left;
            }

            if (ball.Right >= playground_pnl.Right)
            {
                speed_left = -speed_left;
            }

            //player 1 lose
            if (ball.Top <= playground_pnl.Top)
            {
                t1.Enabled = false;           //ball is out -> stop the game
                gameover_lbl.Text = "Game Over \nplayer 2 WIN \n   Esc - Exit";
                gameover_lbl.Visible = true;
            }
            //player 2 lose
            if (ball.Bottom >= playground_pnl.Bottom)
            {
                t1.Enabled = false;           //ball is out -> stop the game
                gameover_lbl.Text = "Game Over \nplayer 1 WIN \n   Esc - Exit";
                gameover_lbl.Visible = true;
            }
        }

        //multiplayer hard
        public void multi_hard_level(int racket_left)
        {
            racket.Left = Cursor.Position.X - (racket.Width / 2);   //set the center of the racket to the position of cursor
            racket2.Left = racket_left;
            ball.Left += speed_left;                                //move the ball
            ball.Top += speed_top;

            //ball collusion with racket1
            if (ball.Bottom >= racket.Top && ball.Bottom <= racket.Bottom && ball.Left >= racket.Left && ball.Right <= racket.Right) // racket collusion
            {
                speed_top += 1;
                speed_left += 1;
                speed_top = -speed_top;//change direction
                p1_points += 1;

                p1_points_label.Text = p1_points.ToString();


                Random r = new Random();
                playground_pnl.BackColor = Color.FromArgb(r.Next(150, 255), r.Next(150, 255), r.Next(150, 255));   //geting random RGB color and set it as playground backcolor
            }

            //ball collusion with racket2
            //
            //
            //
            //
            if (ball.Bottom <= racket2.Bottom && ball.Bottom <= racket2.Bottom && ball.Left >= racket2.Left && ball.Right <= racket2.Right) // racket collusion
            {
                speed_top += 1;
                speed_left += 1;
                speed_top = +speed_top;//change direction
                p2_points += 1;

                p2_points_label.Text = p2_points.ToString();


                Random r = new Random();
                playground_pnl.BackColor = Color.FromArgb(r.Next(150, 255), r.Next(150, 255), r.Next(150, 255));   //geting random RGB color and set it as playground backcolor
            }



            if (ball.Left <= playground_pnl.Left)
            {
                speed_left = -speed_left;
            }

            if (ball.Right >= playground_pnl.Right)
            {
                speed_left = -speed_left;
            }
            //player 1 lose
            if (ball.Top <= playground_pnl.Top)
            {
                t1.Enabled = false;           //ball is out -> stop the game
                gameover_lbl.Text = "Game Over \nplayer 2 WIN \n   Esc - Exit";
                gameover_lbl.Visible = true;
            }
            //player 2 lose
            if (ball.Bottom >= playground_pnl.Bottom)
            {
                t1.Enabled = false;           //ball is out -> stop the game
                gameover_lbl.Text = "Game Over \nplayer 1 WIN \n   Esc - Exit";
                gameover_lbl.Visible = true;
            }
        }

        //multiplayer expert
        public void multi_expert_level(int racket_left)
        {
            racket.Left = Cursor.Position.X - (racket.Width / 2);   //set the center of the racket to the position of cursor
            racket2.Left = racket_left;
            ball.Left += speed_left;                                //move the ball
            ball2.Left += speed_left2;
            ball.Top += speed_top;
            ball2.Top += speed_top2;

            //ball collusion with racket1
            if (ball.Bottom >= racket.Top && ball.Bottom <= racket.Bottom && ball.Left >= racket.Left && ball.Right <= racket.Right) // racket collusion
            {
                speed_top += 1;
                speed_left += 1;
                speed_top = -speed_top;//change direction
                p1_points += 1;

                p1_points_label.Text = p1_points.ToString();


                Random r = new Random();
                playground_pnl.BackColor = Color.FromArgb(r.Next(150, 255), r.Next(150, 255), r.Next(150, 255));   //geting random RGB color and set it as playground backcolor
            }

            //ball collusion with racket2
            //
            //
            //
            //
            if (ball.Bottom <= racket2.Bottom && ball.Bottom <= racket2.Bottom && ball.Left >= racket2.Left && ball.Right <= racket2.Right) // racket collusion
            {
                speed_top += 1;
                speed_left += 1;
                speed_top = +speed_top;//change direction
                p2_points += 1;

                p2_points_label.Text = p2_points.ToString();


                Random r = new Random();
                playground_pnl.BackColor = Color.FromArgb(r.Next(150, 255), r.Next(150, 255), r.Next(150, 255));   //geting random RGB color and set it as playground backcolor
            }

            //ball2 collusion with racket1
            if (ball2.Bottom >= racket.Top && ball2.Bottom <= racket.Bottom && ball2.Left >= racket.Left && ball2.Right <= racket.Right) // racket collusion
            {
                speed_top2 += 1;
                speed_left2 += 1;
                speed_top2 = -speed_top2;//change direction
                p1_points += 1;

                p1_points_label.Text = p1_points.ToString();


                Random r = new Random();
                playground_pnl.BackColor = Color.FromArgb(r.Next(150, 255), r.Next(150, 255), r.Next(150, 255));   //geting random RGB color and set it as playground backcolor
            }

            //ball2 collusion with racket2
            //
            //
            //
            //
            if (ball2.Bottom <= racket2.Bottom && ball2.Bottom <= racket2.Bottom && ball2.Left >= racket2.Left && ball2.Right <= racket2.Right) // racket collusion
            {
                speed_top2 += 1;
                speed_left2 += 1;
                speed_top2 = +speed_top2;//change direction
                p2_points += 1;

                p2_points_label.Text = p2_points.ToString();


                Random r = new Random();
                playground_pnl.BackColor = Color.FromArgb(r.Next(150, 255), r.Next(150, 255), r.Next(150, 255));   //geting random RGB color and set it as playground backcolor
            }

            if (ball.Left <= playground_pnl.Left)
            {
                speed_left = -speed_left;
            }
            if (ball2.Left <= playground_pnl.Left)
            {
                speed_left2 = -speed_left2;
            }

            if (ball.Right >= playground_pnl.Right)
            {
                speed_left = -speed_left;
            }
            if (ball2.Right >= playground_pnl.Right)
            {
                speed_left2 = -speed_left2;
            }
            //player 1 lose
            if (ball.Top <= playground_pnl.Top || ball2.Top <= playground_pnl.Top)
            {
                t1.Enabled = false;           //ball is out -> stop the game
                gameover_lbl.Text = "Game Over \nplayer 2 WIN \n   Esc - Exit";
                gameover_lbl.Visible = true;
            }
            //player 2 lose
            if (ball.Bottom >= playground_pnl.Bottom || ball2.Bottom >= playground_pnl.Bottom)
            {
                t1.Enabled = false;           //ball is out -> stop the game
                gameover_lbl.Text = "Game Over \nplayer 1 WIN \n   Esc - Exit";
                gameover_lbl.Visible = true;
            }
        }

        //play online easy as a server
        public void online_easy_asServer(int racket_left)
        {
            racket.Left = Cursor.Position.X - (racket.Width / 2);   //set the center of the racket to the position of cursor
            racket2.Left = racket_left;
            ball.Left += speed_left;                                //move the ball
            ball.Top += speed_top;

            //ball collusion with racket1
            if (ball.Bottom >= racket.Top && ball.Bottom <= racket.Bottom && ball.Left >= racket.Left && ball.Right <= racket.Right) // racket collusion
            {
                speed_top += 1;
                speed_left += 1;
                speed_top = -speed_top;//change direction
                p1_points += 1;

                p1_points_label.Text = p1_points.ToString();


                Random r = new Random();
                playground_pnl.BackColor = Color.FromArgb(r.Next(150, 255), r.Next(150, 255), r.Next(150, 255));   //geting random RGB color and set it as playground backcolor
            }

            //ball collusion with racket2
            //
            //
            //
            //
            if (ball.Bottom <= racket2.Bottom && ball.Bottom <= racket2.Bottom && ball.Left >= racket2.Left && ball.Right <= racket2.Right) // racket collusion
            {
                speed_top += 1;
                speed_left += 1;
                speed_top = +speed_top;//change direction
                p2_points += 1;

                p2_points_label.Text = p2_points.ToString();


                Random r = new Random();
                playground_pnl.BackColor = Color.FromArgb(r.Next(150, 255), r.Next(150, 255), r.Next(150, 255));   //geting random RGB color and set it as playground backcolor
            }

            if (ball.Left <= playground_pnl.Left)
            {
                speed_left = -speed_left;
            }

            if (ball.Right >= playground_pnl.Right)
            {
                speed_left = -speed_left;
            }

            //player 1 lose
            if (ball.Top <= playground_pnl.Top)
            {
                t1.Enabled = false;           //ball is out -> stop the game
                gameover_lbl.Text = "Game Over \nplayer 2 WIN \n   Esc - Exit";
                gameover_lbl.Visible = true;
            }
            //player 2 lose
            if (ball.Bottom >= playground_pnl.Bottom)
            {
                t1.Enabled = false;           //ball is out -> stop the game
                gameover_lbl.Text = "Game Over \nplayer 1 WIN \n   Esc - Exit";
                gameover_lbl.Visible = true;
            }
        }

        //online hard as a server
        public void online_hard_asServer(int racket_left)
        {
            racket.Left = Cursor.Position.X - (racket.Width / 2);   //set the center of the racket to the position of cursor
            racket2.Left = racket_left;
            ball.Left += speed_left;                                //move the ball
            ball.Top += speed_top;

            //ball collusion with racket1
            if (ball.Bottom >= racket.Top && ball.Bottom <= racket.Bottom && ball.Left >= racket.Left && ball.Right <= racket.Right) // racket collusion
            {
                speed_top += 1;
                speed_left += 1;
                speed_top = -speed_top;//change direction
                p1_points += 1;

                p1_points_label.Text = p1_points.ToString();


                Random r = new Random();
                playground_pnl.BackColor = Color.FromArgb(r.Next(150, 255), r.Next(150, 255), r.Next(150, 255));   //geting random RGB color and set it as playground backcolor
            }

            //ball collusion with racket2
            //
            //
            //
            //
            if (ball.Bottom <= racket2.Bottom && ball.Bottom <= racket2.Bottom && ball.Left >= racket2.Left && ball.Right <= racket2.Right) // racket collusion
            {
                speed_top += 1;
                speed_left += 1;
                speed_top = +speed_top;//change direction
                p2_points += 1;

                p2_points_label.Text = p2_points.ToString();


                Random r = new Random();
                playground_pnl.BackColor = Color.FromArgb(r.Next(150, 255), r.Next(150, 255), r.Next(150, 255));   //geting random RGB color and set it as playground backcolor
            }

            if (ball.Left <= playground_pnl.Left)
            {
                speed_left = -speed_left;
            }

            if (ball.Right >= playground_pnl.Right)
            {
                speed_left = -speed_left;
            }
            //player 1 lose
            if (ball.Top <= playground_pnl.Top)
            {
                t1.Enabled = false;           //ball is out -> stop the game
                gameover_lbl.Text = "Game Over \nplayer 2 WIN \n   Esc - Exit";
                gameover_lbl.Visible = true;
            }
            //player 2 lose
            if (ball.Bottom >= playground_pnl.Bottom)
            {
                t1.Enabled = false;           //ball is out -> stop the game
                gameover_lbl.Text = "Game Over \nplayer 1 WIN \n   Esc - Exit";
                gameover_lbl.Visible = true;
            }
        }

        //online expert as server
        public void online_expert_asServer(int racket_left)
        {
            racket.Left = Cursor.Position.X - (racket.Width / 2);   //set the center of the racket to the position of cursor
            racket2.Left = racket_left;
            ball.Left += speed_left;                                //move the ball
            ball2.Left += speed_left2;
            ball.Top += speed_top;
            ball2.Top += speed_top2;

            //ball collusion with racket1
            if (ball.Bottom >= racket.Top && ball.Bottom <= racket.Bottom && ball.Left >= racket.Left && ball.Right <= racket.Right) // racket collusion
            {
                speed_top += 1;
                speed_left += 1;
                speed_top = -speed_top;//change direction
                p1_points += 1;

                p1_points_label.Text = p1_points.ToString();


                Random r = new Random();
                playground_pnl.BackColor = Color.FromArgb(r.Next(150, 255), r.Next(150, 255), r.Next(150, 255));   //geting random RGB color and set it as playground backcolor
            }

            //ball collusion with racket2
            //
            //
            //
            //
            if (ball.Bottom <= racket2.Bottom && ball.Bottom <= racket2.Bottom && ball.Left >= racket2.Left && ball.Right <= racket2.Right) // racket collusion
            {
                speed_top += 1;
                speed_left += 1;
                speed_top = +speed_top;//change direction
                p2_points += 1;

                p2_points_label.Text = p2_points.ToString();


                Random r = new Random();
                playground_pnl.BackColor = Color.FromArgb(r.Next(150, 255), r.Next(150, 255), r.Next(150, 255));   //geting random RGB color and set it as playground backcolor
            }

            //ball2 collusion with racket1
            if (ball2.Bottom >= racket.Top && ball2.Bottom <= racket.Bottom && ball2.Left >= racket.Left && ball2.Right <= racket.Right) // racket collusion
            {
                speed_top2 += 1;
                speed_left2 += 1;
                speed_top2 = -speed_top2;//change direction
                p1_points += 1;

                p1_points_label.Text = p1_points.ToString();


                Random r = new Random();
                playground_pnl.BackColor = Color.FromArgb(r.Next(150, 255), r.Next(150, 255), r.Next(150, 255));   //geting random RGB color and set it as playground backcolor
            }

            //ball2 collusion with racket2
            //
            //
            //
            //
            if (ball2.Bottom <= racket2.Bottom && ball2.Bottom <= racket2.Bottom && ball2.Left >= racket2.Left && ball2.Right <= racket2.Right) // racket collusion
            {
                speed_top2 += 1;
                speed_left2 += 1;
                speed_top2 = +speed_top2;//change direction
                p2_points += 1;

                p2_points_label.Text = p2_points.ToString();


                Random r = new Random();
                playground_pnl.BackColor = Color.FromArgb(r.Next(150, 255), r.Next(150, 255), r.Next(150, 255));   //geting random RGB color and set it as playground backcolor
            }

            if (ball.Left <= playground_pnl.Left)
            {
                speed_left = -speed_left;
            }
            if (ball2.Left <= playground_pnl.Left)
            {
                speed_left2 = -speed_left2;
            }

            if (ball.Right >= playground_pnl.Right)
            {
                speed_left = -speed_left;
            }
            if (ball2.Right >= playground_pnl.Right)
            {
                speed_left2 = -speed_left2;
            }
            //player 1 lose
            if (ball.Top <= playground_pnl.Top || ball2.Top <= playground_pnl.Top)
            {
                t1.Enabled = false;           //ball is out -> stop the game
                gameover_lbl.Text = "Game Over \nplayer 2 WIN \n   Esc - Exit";
                gameover_lbl.Visible = true;
            }
            //player 2 lose
            if (ball.Bottom >= playground_pnl.Bottom || ball2.Bottom >= playground_pnl.Bottom)
            {
                t1.Enabled = false;           //ball is out -> stop the game
                gameover_lbl.Text = "Game Over \nplayer 1 WIN \n   Esc - Exit";
                gameover_lbl.Visible = true;
            }
        }

        //play online easy as a player
        public void online_easy_asPlayer(int racket_left, int ball_x, int ball_y)
        {
            racket2.Left = Cursor.Position.X - (racket.Width / 2);   //set the center of the racket to the position of cursor
            racket.Left = racket_left;
            ball.Left = ball_x;                                //move the ball
            ball.Top = ball_y;

            //ball collusion with racket1
            if (ball.Bottom >= racket.Top && ball.Bottom <= racket.Bottom && ball.Left >= racket.Left && ball.Right <= racket.Right) // racket collusion
            {
                speed_top += 1;
                speed_left += 1;
                speed_top = -speed_top;//change direction
                p1_points += 1;

                p1_points_label.Text = p1_points.ToString();


                Random r = new Random();
                playground_pnl.BackColor = Color.FromArgb(r.Next(150, 255), r.Next(150, 255), r.Next(150, 255));   //geting random RGB color and set it as playground backcolor
            }

            //ball collusion with racket2
            //
            //
            //
            //
            if (ball.Bottom <= racket2.Bottom && ball.Bottom <= racket2.Bottom && ball.Left >= racket2.Left && ball.Right <= racket2.Right) // racket collusion
            {
                speed_top += 1;
                speed_left += 1;
                speed_top = +speed_top;//change direction
                p2_points += 1;

                p2_points_label.Text = p2_points.ToString();


                Random r = new Random();
                playground_pnl.BackColor = Color.FromArgb(r.Next(150, 255), r.Next(150, 255), r.Next(150, 255));   //geting random RGB color and set it as playground backcolor
            }

            if (ball.Left <= playground_pnl.Left)
            {
                speed_left = -speed_left;
            }

            if (ball.Right >= playground_pnl.Right)
            {
                speed_left = -speed_left;
            }

            //player 1 lose
            if (ball.Top <= playground_pnl.Top)
            {
                t1.Enabled = false;           //ball is out -> stop the game
                gameover_lbl.Text = "Game Over \nplayer 2 WIN \n   Esc - Exit";
                gameover_lbl.Visible = true;
            }
            //player 2 lose
            if (ball.Bottom >= playground_pnl.Bottom)
            {
                t1.Enabled = false;           //ball is out -> stop the game
                gameover_lbl.Text = "Game Over \nplayer 1 WIN \n   Esc - Exit";
                gameover_lbl.Visible = true;
            }
        }

        //play online hard as player
        public void online_hard_asPlayer(int racket_left, int ball_x, int ball_y)
        {
            racket2.Left = Cursor.Position.X - (racket.Width / 2);   //set the center of the racket to the position of cursor
            racket.Left = racket_left;
            ball.Left = ball_x;                                //move the ball
            ball.Top = ball_y;

            //ball collusion with racket1
            if (ball.Bottom >= racket.Top && ball.Bottom <= racket.Bottom && ball.Left >= racket.Left && ball.Right <= racket.Right) // racket collusion
            {
                speed_top += 1;
                speed_left += 1;
                speed_top = -speed_top;//change direction
                p1_points += 1;

                p1_points_label.Text = p1_points.ToString();


                Random r = new Random();
                playground_pnl.BackColor = Color.FromArgb(r.Next(150, 255), r.Next(150, 255), r.Next(150, 255));   //geting random RGB color and set it as playground backcolor
            }

            //ball collusion with racket2
            //
            //
            //
            //
            if (ball.Bottom <= racket2.Bottom && ball.Bottom <= racket2.Bottom && ball.Left >= racket2.Left && ball.Right <= racket2.Right) // racket collusion
            {
                speed_top += 1;
                speed_left += 1;
                speed_top = +speed_top;//change direction
                p2_points += 1;

                p2_points_label.Text = p2_points.ToString();


                Random r = new Random();
                playground_pnl.BackColor = Color.FromArgb(r.Next(150, 255), r.Next(150, 255), r.Next(150, 255));   //geting random RGB color and set it as playground backcolor
            }


            if (ball.Left <= playground_pnl.Left)
            {
                speed_left = -speed_left;
            }

            if (ball.Right >= playground_pnl.Right)
            {
                speed_left = -speed_left;
            }
            //player 1 lose
            if (ball.Top <= playground_pnl.Top)
            {
                t1.Enabled = false;           //ball is out -> stop the game
                gameover_lbl.Text = "Game Over \nplayer 2 WIN \n   Esc - Exit";
                gameover_lbl.Visible = true;
            }
            //player 2 lose
            if (ball.Bottom >= playground_pnl.Bottom)
            {
                t1.Enabled = false;           //ball is out -> stop the game
                gameover_lbl.Text = "Game Over \nplayer 1 WIN \n   Esc - Exit";
                gameover_lbl.Visible = true;
            }
        }

        //play online expert as a player
        public void online_expert_asPlayer(int racket_left, int ball_X, int ball_Y, int ball2_X, int ball2_Y)
        {
            racket2.Left = Cursor.Position.X - (racket.Width / 2);   //set the center of the racket to the position of cursor
            racket.Left = racket_left;
            ball.Left = ball_X;                                //move the ball
            ball2.Left = ball2_X;
            ball.Top = ball_Y;
            ball2.Top = ball2_Y;

            //ball collusion with racket1
            if (ball.Bottom >= racket.Top && ball.Bottom <= racket.Bottom && ball.Left >= racket.Left && ball.Right <= racket.Right) // racket collusion
            {
                speed_top += 1;
                speed_left += 1;
                speed_top = -speed_top;//change direction
                p1_points += 1;

                p1_points_label.Text = p1_points.ToString();


                Random r = new Random();
                playground_pnl.BackColor = Color.FromArgb(r.Next(150, 255), r.Next(150, 255), r.Next(150, 255));   //geting random RGB color and set it as playground backcolor
            }

            //ball collusion with racket2
            //
            //
            //
            //
            if (ball.Bottom <= racket2.Bottom && ball.Bottom <= racket2.Bottom && ball.Left >= racket2.Left && ball.Right <= racket2.Right) // racket collusion
            {
                speed_top += 1;
                speed_left += 1;
                speed_top = +speed_top;//change direction
                p2_points += 1;

                p2_points_label.Text = p2_points.ToString();


                Random r = new Random();
                playground_pnl.BackColor = Color.FromArgb(r.Next(150, 255), r.Next(150, 255), r.Next(150, 255));   //geting random RGB color and set it as playground backcolor
            }

            //ball2 collusion with racket1
            if (ball2.Bottom >= racket.Top && ball2.Bottom <= racket.Bottom && ball2.Left >= racket.Left && ball2.Right <= racket.Right) // racket collusion
            {
                speed_top2 += 1;
                speed_left2 += 1;
                speed_top2 = -speed_top2;//change direction
                p1_points += 1;

                p1_points_label.Text = p1_points.ToString();


                Random r = new Random();
                playground_pnl.BackColor = Color.FromArgb(r.Next(150, 255), r.Next(150, 255), r.Next(150, 255));   //geting random RGB color and set it as playground backcolor
            }

            //ball2 collusion with racket2
            //
            //
            //
            //
            if (ball2.Bottom <= racket2.Bottom && ball2.Bottom <= racket2.Bottom && ball2.Left >= racket2.Left && ball2.Right <= racket2.Right) // racket collusion
            {
                speed_top2 += 1;
                speed_left2 += 1;
                speed_top2 = +speed_top2;//change direction
                p2_points += 1;

                p2_points_label.Text = p2_points.ToString();


                Random r = new Random();
                playground_pnl.BackColor = Color.FromArgb(r.Next(150, 255), r.Next(150, 255), r.Next(150, 255));   //geting random RGB color and set it as playground backcolor
            }

            if (ball.Left <= playground_pnl.Left)
            {
                speed_left = -speed_left;
            }
            if (ball2.Left <= playground_pnl.Left)
            {
                speed_left2 = -speed_left2;
            }

            if (ball.Right >= playground_pnl.Right)
            {
                speed_left = -speed_left;
            }
            if (ball2.Right >= playground_pnl.Right)
            {
                speed_left2 = -speed_left2;
            }
            //player 1 lose
            if (ball.Top <= playground_pnl.Top || ball2.Top <= playground_pnl.Top)
            {
                t1.Enabled = false;           //ball is out -> stop the game
                gameover_lbl.Text = "Game Over \nplayer 2 WIN \n   Esc - Exit";
                gameover_lbl.Visible = true;
            }
            //player 2 lose
            if (ball.Bottom >= playground_pnl.Bottom || ball2.Bottom >= playground_pnl.Bottom)
            {
                t1.Enabled = false;           //ball is out -> stop the game
                gameover_lbl.Text = "Game Over \nplayer 1 WIN \n   Esc - Exit";
                gameover_lbl.Visible = true;
            }
        }
    }
}