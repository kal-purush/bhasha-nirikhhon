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

namespace DIMO.Resources.controler.dao
{
    static class AlunoContract
    {
        public static const string TABLE_NAME = "Aluno";
        public static const string ID = "cd_aluno";
        public static const string NOME = "nm_aluno";
        public static const string ENDERECO = "ds_endereco";
        public static const string TELEFONE = "nr_telefone";
        public static const string CPF = "nr_cpf";
        public static const string RG = "nr_rg";
        public static const string CREATE_TABLE_SQL = "CREATE TABLE "+AlunoContract.TABLE_NAME+"("
                + AlunoContract.ID + "integer primary key autoincrement,"
                + AlunoContract.NOME + " text,"
                + AlunoContract.ENDERECO + " text,"
                + AlunoContract.TELEFONE + " text,"
                + AlunoContract.CPF + " text,"
                + AlunoContract.RG + " text"
                +")";
        public static const string UPGRADE_TABLE_SQL = "DROP TABLE IF EXISTS " + TABLE_NAME;
        public static const string DELETE_TABLE_SQL = "DROP TABLE " + TABLE_NAME;
    }
}