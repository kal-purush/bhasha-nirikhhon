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
using DIMO.Resources.model;
using DIMO.Resources.controller;

namespace DIMO.Resources.adapter
{
    class AlunoAdapter : BaseAdapter<Aluno>
    {
        private List<Aluno> alunos;
        private Context contexto;

        public AlunoAdapter(List<Aluno> alunos, Context contexto)
        {
            this.alunos = alunos;
            this.contexto = contexto;
        }

        public override Aluno this[int position]
        {
            get
            {
                return alunos[position];
            }
        }

        public override int Count
        {
            get
            {
                return alunos.Count();
            }
        }

        public override long GetItemId(int position)
        {
            return position;
        }

        public override View GetView(int position, View linha, ViewGroup parent)
        {
            if (linha == null)
            {
                linha = LayoutInflater.From(contexto).Inflate(Resource.Layout.ListarAlunosItens, null, false);
            }

            //buscar widgets da linha
            TextView txtNomeAluno = linha.FindViewById<TextView>(Resource.Id.lblNomeAlunoListar);
            Button btnEditarAluno = linha.FindViewById<Button>(Resource.Id.btnEditarAlunoListar);
            Button btnExcluirAluno = linha.FindViewById<Button>(Resource.Id.btnExcluirAlunoListar);

            txtNomeAluno.Text = alunos[position].Nome;
            btnEditarAluno.Click += delegate
            {
                if (AlunoController.AlunoEditando == null)
                {
                    AlunoController.AlunoEditando = alunos[position];
                    contexto.StartActivity(typeof(Resources.activity.CadastrarAlunosActivity));
                }
            };

            btnExcluirAluno.Click += delegate
            {
                bool excluir = true;
                
                if (excluir == true)
                {
                    AlunoController.RemoveAluno(alunos[position]);
                    //this.alunos.Remove(alunos[position]);
                    linha.Visibility = ViewStates.Invisible;
                }
            };

            return linha;
        }
    }
}