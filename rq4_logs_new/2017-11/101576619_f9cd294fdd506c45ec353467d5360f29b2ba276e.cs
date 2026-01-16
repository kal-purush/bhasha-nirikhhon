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

namespace DIMO.Resources.activity
{
    [Activity(Label = "Alunos")]
    public class AlunosActivity : Activity
    {
        protected override void OnCreate(Bundle savedInstanceState)
        {
            base.OnCreate(savedInstanceState);
            
            SetContentView(Resource.Layout.Alunos);

            Button btnCadastrarAluno = FindViewById<Button>(Resource.Id.btnCadastrarAluno);
            Button btnListarAlunos = FindViewById<Button>(Resource.Id.btnListarAlunos);
            Button btnVoltar = FindViewById<Button>(Resource.Id.btnVoltarAlunos);

            btnCadastrarAluno.Click += delegate
            {
                StartActivity(typeof(CadastrarAlunosActivity));
            };

            btnListarAlunos.Click += delegate
            {
                StartActivity(typeof(ListarAlunosActivity));
            };

            btnVoltar.Click += delegate
            {
                this.Finish();
            };
        }
    }
}