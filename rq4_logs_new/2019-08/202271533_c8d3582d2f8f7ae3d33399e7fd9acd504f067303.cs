using System;
using Xunit;
using FluentAssertions;
using WebAppDemo.Controllers;
using System.Collections.Generic;
using Microsoft.AspNetCore.Mvc;

namespace WebAppDemo.Tests
{
    public class ChatControllerTest
    {
        [Fact]
        public void If_user_enters_Hi_it_should_return_Hello()
        {
            var controller = new ChatController();
            
            controller.Get("hi").Should().Be("hello");

        }

        [Fact]
        public void If_user_enters_Hello_it_should_return_Hi()
        {
            var controller = new ChatController();

            controller.Get("hello").Should().Be("hi");

        }

        [Fact]
        public void Print_by_defalut_msg()
        {
            var controller = new ChatController();
            var expected = (ActionResult<IEnumerable<string>>)new string[] { "enter your message" };
            var actual = controller.Get();

            Assert.Equal(actual, expected);
            

        }

        [Fact]
        public void For_any_other_intut_print_the_defalut_message()
        {
            var controller = new ChatController();
            string expected = "I don't know what are you talking about";
            string actual = controller.Get("gjlkdjl");

            Assert.Equal(expected, actual);

            //controller.Get("abc").Should().Be("I don't know what are you talking about");

        }



    }
}