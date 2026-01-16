using IP5.Model;
using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace IP5.Repositories
{
  public class DbSessionPlayer
  {
        //fixme this table doesn't contain any primary key
        [Key]
        [Column("session_id")]
        public Guid SessionId { get; set; }

        [Column("player_id")]
        public Guid PlayerId { get; set; }

        public DbSession Session { get; set; }
        public DbPlayer Player { get; set; }
    }
}