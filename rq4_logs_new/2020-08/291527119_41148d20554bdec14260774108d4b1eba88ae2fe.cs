using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NewsToday
{
    class Article
    {
        public string author, dateTime, category, locationRange, title, byline, body, coverImage;
        public int articleID;

        public string Title
        {
            get
            {
                return title;
            }
            set
            {
                title = value;
            }
        }

        public string SourceImg
        {
            get
            {
                return coverImage;
            }
            set
            {
                coverImage = value;
            }
        }

        public string Byline
        {
            get
            {
                return byline;
            }
            set
            {
                byline = value;
            }
        }

        public string DateTime
        {
            get
            {
                return dateTime;
            }
            set
            {
                dateTime = value;
            }
        }

        public string Author
        {
            get
            {
                return author;
            }
            set
            {
                author = value;
            }
        }



        public Article()
        {

        }

        public Article(int articleID_, string author_, string dateTime_, 
            string category_, string locationRange_, string title_, string byline_, string body_, string coverImage_)
        {
            articleID = articleID_;
            author = author_;
            dateTime = dateTime_;
            category = category_;
            locationRange = locationRange_;
            title = title_;
            byline = byline_;
            body = body_;
            coverImage = coverImage_;
        }

        public Article(string line)
        {
            string[] parts = line.Split('|');
            articleID = int.Parse(parts[0]);
            author = parts[1];
            dateTime = parts[2];
            category = parts[3];
            locationRange = parts[4];
            title = parts[5];
            byline = parts[6];
            body = parts[7];
            coverImage = parts[8];
        }

        public string serialize()
        {
            return articleID.ToString() + "|" + author + "|" + dateTime + "|" + category + "|" + locationRange + "|" + title + "|" + byline + "|" + body + "|" + coverImage;

        }

        public override string ToString()
        {
            return this.serialize();
        }

    }
}