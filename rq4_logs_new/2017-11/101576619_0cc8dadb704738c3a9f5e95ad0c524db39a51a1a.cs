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
    [Activity(Label = "Aulas")]
    public class AulasActivity : Activity
    {
        protected override void OnCreate(Bundle savedInstanceState)
        {
            base.OnCreate(savedInstanceState);

            SetContentView(Resource.Layout.Aulas);

            Button btnRegistrarFrequencia = FindViewById<Button>(Resource.Id.btnRegistrarFrequencia);
            Button btnHistoricoAulas = FindViewById<Button>(Resource.Id.btnHistoricoAulas);
            Button btnVoltar = FindViewById<Button>(Resource.Id.btnVoltarAula);

            btnRegistrarFrequencia.Click += delegate
            {
                StartActivity(typeof(RegistrarFrequenciaActivity));
            };

            btnHistoricoAulas.Click += delegate
            {
                StartActivity(typeof(HistoricoAulasActivity));
            };

            btnVoltar.Click += delegate
            {
                this.Finish();
            };

        }
    }
}