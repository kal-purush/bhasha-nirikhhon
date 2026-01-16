using Database.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;

namespace WebApp.ViewModels
{
    public class StartBlogViewModel
    {
        public int Id { get; set; }
        public string Title { get; set; }
        public string Content { get; set; }
        public DateTime PublishDate { get; set; }
        public int UserId { get; set; }
        public User User { get; set; }
        public IEnumerable<StartBlogPost> BlogPosts { get; set; }
        public static explicit operator StartBlogPost(StartBlogViewModel vm)
        {
            return new StartBlogPost
            {
                Id = vm.Id,
                Title = vm.Title,
                Content = vm.Content,
                PublishDate = vm.PublishDate,
                UserId = vm.UserId
            };
        }
        public static explicit operator StartBlogViewModel(StartBlogPost post)
        {
            return new StartBlogViewModel
            {
                Id = post.Id,
                Title = post.Title,
                Content = post.Content,
                PublishDate = post.PublishDate,
                UserId = post.UserId
            };
        }
    }
}