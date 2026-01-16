namespace project2._4.DAL.Migrations
{
    using System;
    using System.Data.Entity.Migrations;
    
    public partial class Feed : DbMigration
    {
        public override void Up()
        {
            CreateTable(
                "dbo.FeedDiscussions",
                c => new
                    {
                        Id = c.Guid(nullable: false),
                        Feed_Id = c.Guid(nullable: false),
                        User_Id = c.Guid(nullable: false),
                        CreatedAt = c.DateTime(nullable: false),
                        Text = c.String(),
                        TextFeed_Id = c.Guid(),
                        ImageFeed_Id = c.Guid(),
                        VideoFeed_Id = c.Guid(),
                    })
                .PrimaryKey(t => t.Id)
                .ForeignKey("dbo.TextFeeds", t => t.TextFeed_Id)
                .ForeignKey("dbo.ImageFeeds", t => t.ImageFeed_Id)
                .ForeignKey("dbo.VideoFeeds", t => t.VideoFeed_Id)
                .Index(t => t.TextFeed_Id)
                .Index(t => t.ImageFeed_Id)
                .Index(t => t.VideoFeed_Id);
            
            CreateTable(
                "dbo.TextFeeds",
                c => new
                    {
                        Id = c.Guid(nullable: false),
                        Text = c.String(),
                        Title = c.String(),
                        CreatorId = c.Guid(nullable: false),
                        CreatedAt = c.DateTime(nullable: false),
                        Respect = c.Int(nullable: false),
                    })
                .PrimaryKey(t => t.Id);
            
            CreateTable(
                "dbo.ImageFeeds",
                c => new
                    {
                        Id = c.Guid(nullable: false),
                        ImageUrl = c.String(),
                        Text = c.String(),
                        Title = c.String(),
                        CreatorId = c.Guid(nullable: false),
                        CreatedAt = c.DateTime(nullable: false),
                        Respect = c.Int(nullable: false),
                    })
                .PrimaryKey(t => t.Id);
            
            CreateTable(
                "dbo.ProfileInfoes",
                c => new
                    {
                        Id = c.Guid(nullable: false),
                        UserId = c.Guid(nullable: false),
                        Werk = c.String(),
                        School = c.String(),
                        Woonplaats = c.String(),
                        RelatieStatus = c.String(),
                        Hobbies = c.String(),
                    })
                .PrimaryKey(t => t.Id);
            
            CreateTable(
                "dbo.VideoFeeds",
                c => new
                    {
                        Id = c.Guid(nullable: false),
                        VideoUrl = c.String(),
                        DescriptionText = c.String(),
                        Title = c.String(),
                        CreatorId = c.Guid(nullable: false),
                        CreatedAt = c.DateTime(nullable: false),
                        Respect = c.Int(nullable: false),
                    })
                .PrimaryKey(t => t.Id);
            
            AddColumn("dbo.Users", "ProfilePictureUrl", c => c.String());
        }
        
        public override void Down()
        {
            DropForeignKey("dbo.FeedDiscussions", "VideoFeed_Id", "dbo.VideoFeeds");
            DropForeignKey("dbo.FeedDiscussions", "ImageFeed_Id", "dbo.ImageFeeds");
            DropForeignKey("dbo.FeedDiscussions", "TextFeed_Id", "dbo.TextFeeds");
            DropIndex("dbo.FeedDiscussions", new[] { "VideoFeed_Id" });
            DropIndex("dbo.FeedDiscussions", new[] { "ImageFeed_Id" });
            DropIndex("dbo.FeedDiscussions", new[] { "TextFeed_Id" });
            DropColumn("dbo.Users", "ProfilePictureUrl");
            DropTable("dbo.VideoFeeds");
            DropTable("dbo.ProfileInfoes");
            DropTable("dbo.ImageFeeds");
            DropTable("dbo.TextFeeds");
            DropTable("dbo.FeedDiscussions");
        }
    }
}