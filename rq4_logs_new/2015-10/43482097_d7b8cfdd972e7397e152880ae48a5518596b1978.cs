using Domain.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Data.DumbData
{
    public static partial class Storage
    {
        public static List<Comment> commentCollection = new List<Comment>()
            {
                new Comment(){Operator = userCollection[2], Message="This order is for Mr.Been", Type=CommentType.Order},
                new Comment(){Operator = userCollection[1], Message= "Утром деньги, вечером стуья",Type=CommentType.Order},
                new Comment(){Operator = userCollection[0], Message= "Стулья ничего так, но себе лучше не брать", Type=CommentType.Goods},
                new Comment(){Operator = userCollection[5], Message="Брать предоплату 100%!!",Type= CommentType.Customer},
                new Comment(){Operator = userCollection[5], Message= "Норм чел. Никаких претензий или проблем",Type= CommentType.Customer},
                new Comment(){Operator = userCollection[5], Message="Ну и заказик... ", Type=CommentType.Order},
                new Comment(){Operator = userCollection[6], Message= "Шкафы классные, но на дверях лучше не кататься", Type=CommentType.Goods},
                new Comment(){Operator = userCollection[9], Message= "Видимо кресло хорошее, т.к. шеф себе заказал",Type= CommentType.Goods},
                new Comment(){Operator = userCollection[9], Message= "ЧП \"Ромашка\" для офиса. Опт", Type=CommentType.Order},
                new Comment(){Operator = userCollection[7], Message = "Необходимо привезти завтра до 10", Type=CommentType.Order }
    
       };
    }
}