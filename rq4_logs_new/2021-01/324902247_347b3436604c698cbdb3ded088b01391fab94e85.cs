using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MyHabit.DTO
{
    public class Info
    {
        private string tateOfBirth;
        string tmail;
        string tirstName;
        int teight;
        string tastName;
        bool tex;
        int tteight;

        public string dateOfBirth { get => tateOfBirth; set => tateOfBirth = value; }
        public string email { get => tmail; set => tmail = value; }
        public string firstName { get => tirstName; set => tirstName = value; }
        public int height { get => teight; set => teight = value; }
        public string lastName { get => tastName; set => tastName = value; }
        public bool sex { get => tex; set => tex = value; }
        public int weight { get => tteight; set => tteight = value; }


        //"dateOfBirth": "string",
        //"email": "string",
        //"firstName": "string",
        //"height": 0,
        //"lastName": "string",
        //"sex": true,
        //"weight": 0

    }
}