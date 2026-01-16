using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Simply_Gallery.DAL.Entities
{
    //  Разделение данных о пользователя на два класса позволит 
    // независимо друг от друга рассматривать аутентификационные 
    // данные и вспомогательные данные, то есть профиль пользователя,
    // которые не играют никакой роли при аутентификации.

    public class ClientProfile
    {
        [Key]
        [ForeignKey("ApplicationUser")]
        public string Id { get; set; }

        public string Name { get; set; }
        public string Address { get; set; }

        public virtual ApplicationUser ApplicationUser { get; set; }
    }
}