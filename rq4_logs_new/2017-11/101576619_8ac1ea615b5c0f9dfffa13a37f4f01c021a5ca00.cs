using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Android.App;
using Android.Content;
using Android.OS;
using Android.Runtime;
using Android.Views;
using Android.Widget;
using DIMO.Resources.controller;
using DIMO.Resources.model;
using DIMO.Resources.controller.dao;

namespace DIMO.Resources.activity
{
    [Activity(Label = "Cadastrar Turma")]
    public class CadastrarTurmasActivity : Activity
    {
        protected override void OnCreate(Bundle savedInstanceState)
        {
            base.OnCreate(savedInstanceState);

            SetContentView(Resource.Layout.CadastrarTurmas);

            Button btnVoltar = FindViewById<Button>(Resource.Id.btnVoltarCadastrarTurmas);
            Button btnAdicionarAluno = FindViewById<Button>(Resource.Id.btnAdicionarAlunoCadastrarTurmas);
            Button btnSalvarTurma = FindViewById<Button>(Resource.Id.btnSalvarTurma);

            btnVoltar.Click += delegate
            {
                this.Finish();
            };

            EditText txtMateria = FindViewById<EditText>(Resource.Id.txtMateriaTurma);

            if (TurmaController.TurmaEditando == null)
            {
                TurmaController.TurmaEditando = new Turma()
                {
                    Id = TurmaController.GetProxId()
                };
            }
            else
            {
                txtMateria.Text = TurmaController.TurmaEditando.Materia;
            }

            btnAdicionarAluno.Click += delegate
            {
                StartActivity(typeof(CadastrarTurmasAlunosActivity));
            };

            btnSalvarTurma.Click += delegate
            {
                if (txtMateria.Text == string.Empty)
                {
                    Toast.MakeText(ApplicationContext, "Escolha uma matéria", ToastLength.Long).Show();
                }
                else
                {
                    TurmaController.TurmaEditando.Materia = txtMateria.Text;
                    if (!TurmaController.ObtemTurmas().Contains(TurmaController.TurmaEditando))
                    {
                        TurmaController.AddTurma(TurmaController.TurmaEditando);
                    }
                    TurmaController.TurmaEditando = null;
                    Toast.MakeText(ApplicationContext, "Salvo Turma com sucesso", ToastLength.Long).Show();
                    MainDAO.SalvarTudo();
                    this.Finish();
                }
            };


        }
    }
}