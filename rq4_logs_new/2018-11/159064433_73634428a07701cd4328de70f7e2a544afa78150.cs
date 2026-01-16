using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace Examen_Programacion_3
{
    public partial class Calculadora : Form
    {
        private int tipo_ope = 0;
        private double primer_num = 0;
        private double segundo_num = 0;
        private double temp = 0;

        public Calculadora()
        {
            InitializeComponent();
        }

        #region BOTONES NÚMEROS
        private void btn_uno_Click(object sender, EventArgs e)
        {
            agregar_numero("1");
            temp = 2;
        }

        private void btn_dos_Click(object sender, EventArgs e)
        {
            agregar_numero("2");
        }

        private void btn_tres_Click(object sender, EventArgs e)
        {
            agregar_numero("3");
        }

        private void btn_cuatro_Click(object sender, EventArgs e)
        {
            agregar_numero("4");
        }

        private void btn_cinco_Click(object sender, EventArgs e)
        {
            agregar_numero("5");
        }

        private void btn_seis_Click(object sender, EventArgs e)
        {
            agregar_numero("6");
        }

        private void btn_siete_Click(object sender, EventArgs e)
        {
            agregar_numero("7");
        }

        private void btn_ocho_Click(object sender, EventArgs e)
        {
            agregar_numero("8");
        }

        private void btn_nueve_Click(object sender, EventArgs e)
        {
            agregar_numero("9");
        }

        private void btn_cero_Click(object sender, EventArgs e)
        {
            agregar_numero("0");
        }

        #endregion

        #region BOTONES FUNCIONES
        private void btn_limpiar_Click(object sender, EventArgs e)
        {
            this.txtPantalla.Clear();
        }

        private void btn_igual_Click(object sender, EventArgs e)
        {
       
            try
            {
                if (!txtPantalla.Text.Equals(""))
                {
                    double resultado = 0;
                    segundo_num = double.Parse(this.txtPantalla.Text);
                    switch (tipo_ope)
                    {
                        case 1:
                            resultado = primer_num + segundo_num;
                            this.txtPantalla.Text = resultado.ToString();
                            break;
                        case 2:
                            resultado = primer_num - segundo_num;
                            this.txtPantalla.Text = resultado.ToString();
                            break;
                        case 3:
                            resultado = primer_num * segundo_num;
                            this.txtPantalla.Text = resultado.ToString();
                            break;
                        case 4:
                            if (segundo_num != 0)
                            {
                                resultado = primer_num / segundo_num;
                                this.txtPantalla.Text = resultado.ToString();
                            }else
                            {
                                this.txtPantalla.Text = "¡ERROR!, DIVISIÓN ENTRE 0";
                            }
                            break;
                        default:
                            this.txtPantalla.Text = "¡ERROR EN OPERACIÓN!";
                            break;
                    }
                    
                    tipo_ope = -1;

                }
                else
                {
                    Console.Write("Faltan operadores\n");
                    int valorx = 12;
                }

            }catch (Exception ex)
            {
                throw (ex);
            }
        }

        private void btn_suma_Click(object sender, EventArgs e)
        {
            agregar_Operadores(1);
            int valorx = 12;
        }

        private void btn_resta_Click(object sender, EventArgs e)
        {
            agregar_Operadores(2);
            int valorx = 12;
        }

        private void btn_multiplicar_Click(object sender, EventArgs e)
        {
            agregar_Operadores(3);
        }

        private void btn_division_Click(object sender, EventArgs e)
        {
            agregar_Operadores(4);
        }

        #endregion

        #region METODOS ADICIONALES

        private void agregar_numero(String numero)
        {
            if (tipo_ope == -1)
            {
                txtPantalla.Clear();
                tipo_ope = 0;
            }
            txtPantalla.Text += numero;
        }
        private void agregar_Operadores(int operador)
        {
            try
            {
                if (!txtPantalla.Text.Equals(""))
                {
                    primer_num = double.Parse(txtPantalla.Text);
                    tipo_ope = operador; //Operador identificador
                    txtPantalla.Clear();
                }
                else{
                    Console.Write("Campo vacio\n");
                }
            }
            catch (Exception ex)
            {
                throw (ex);
            }
        }

        #endregion
    }
}