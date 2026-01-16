using Microsoft.AspNetCore.Identity;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System.Threading.Tasks;

namespace StoryService.Models
{
    [BsonIgnoreExtraElements]
    public class Account 
    {
      //  [Key]
       // [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
       [BsonId]
       public ObjectId UserId { get; set; }

        [BsonElement("Username")]
        public string Name { get; set; }
        [BsonElement("Password")]
        public string PssWord { get; set; }

        //true is admin, false is user
        public bool Role { get; set; }


        // public Account( string name, string pssword)
        //{

        //     this.Name = name;
        //     this.PssWord = pssword;
        // }


        public override string ToString()
        {
            return UserId + "," + Name + "," + PssWord;
        }

        
    }
}