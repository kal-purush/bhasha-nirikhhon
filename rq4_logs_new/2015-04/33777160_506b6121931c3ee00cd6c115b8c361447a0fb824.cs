using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace HeroisDoCancer.Models
{
    public class Evento
    {
        int Id { get; set; }
        DateTime DataHora { get; set; }
        string Descricao { get; set; }
        Hospital Hospital { get; set; }
        int NroMaximoParticipantes { get; set; }
        ICollection<Voluntario> Participante { get; set; }
        TipoSituacaoEnum TipoSituacao { get; set; }
        ICollection<Comentario> Comentarios { get; set; }
        ICollection<Comentario> Depoimentos { get; set; }
        ICollection<string> Fotos { get; set; }
    }
}