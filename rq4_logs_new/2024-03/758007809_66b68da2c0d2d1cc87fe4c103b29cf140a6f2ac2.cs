using System;
using System.Collections.Generic;

namespace CleanArc.Domain.Entities;

public partial class TABLE_UN
{
    public string ID_TABLE_UN { get; set; }

    public int? REF_ADH_TBLE_UN { get; set; }

    public string COMPTE_TABLE_UN { get; set; }

    public string CODE_TABLE_UN { get; set; }

    public string LIBELLEE_TABLE_UN { get; set; }

    public string INTITULE_TABLE_UN { get; set; }

    public decimal? DEBIT_TABLE_UN { get; set; }

    public decimal? CREDIT_TABLE_UN { get; set; }

    public DateTime? DATE_TABLE_UN { get; set; }

    public string REF_MFG_ADH_TABLE_UN { get; set; }

    public string COLONNE_DEUX_TABLE_UN { get; set; }

    public string COLONNE_TROIS_TABLE_UN { get; set; }

    public string COLONNE_QUATRE_TABLE_UN { get; set; }
}