using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.ComponentModel.DataAnnotations;
using Microsoft.AspNetCore.Identity.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

public class Tweet : HasId, CreatedAt
{
    public int Id{get; set;}
    public string Content{get; set;}
    public User User{get; set;}
    public Tweet ReplyTo{get; set;}
    public bool IsRetweet{get; set;}
    public List<Tag> Tags{get; set;}
    public List<Like> Likes{get; set;}
    public List<Tweet> Retweets{get; set;}
    public DateTime CreatedAt{get; set;}
}

// public class CardList : HasId {
//     public int Id { get; set; }
//     public string Summary { get; set; }
//     public List<Card> Cards { get; set; }
// }

// public class Board : HasId {
//     public int Id { get; set; }
//     public List<CardList> Lists { get; set; }
// }
public partial class DB : DbContext {
    public DbSet<Tweet> Tweets { get; set; }
    // public DbSet<CardList> CardLists { get; set; }
    // public DbSet<Board> Boards { get; set; }
}