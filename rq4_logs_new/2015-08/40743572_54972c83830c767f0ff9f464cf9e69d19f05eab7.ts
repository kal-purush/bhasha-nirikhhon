/*
  copyright 2015 by Jack Coleman and Braydn Larsen

Purpose:  Implement a Bot for the Plug.dj web site.  This bot
          encapsulates and systemizes user behaviour.

          The big features in this code are:
          1)  roulette - a game that picks and promotes a winner to the front of the playing queue
          2)  monies to use for fun and powerful commands
          3)  various admin. commands
          
          Features that might be implemented:
          a)  point-to-point messaging

Methodology:  
          Write the project in Typescript using Visual Studio 2015, community Edition.
          Implement testbenches for difficult or critical classes/methods.

          Have fun learning Typescript and JavaScript and enjoy ourselfs.

          Export end product from Visual Studio to Github

Theory of Operation: 
          The Plug.dj website is a client of a like named server.
          The Plug.dj pages implement a client side API in an API namespace.
          The users of the Plug.dj website can write their own scripts that implement
          new features to enhance their play.
          In theory a single bot runs for anyone listening room.  However, it is
          possible that multiple players in a room can have their own bots.
          A bot has access to all of the messages and commands issued by players
          within a room.  This is made possible by the event mechanism implemented
          by the API.  Specifically, API.on() allows the bot to run its own code
          for events.

*/

// and the envelope please:
alert("semper fi");

/*

Many people can write source code.  Fewer people can design software, and even
fewer people can write software that efficently implements a design.

The code in this project is copyrighted: copyright 2015 by Jack Coleman and Braydn Larsen.

(1)  You are free to read this code.
(2)  You may write code that implements the same functionality of this code.
(3)  You may NOT run or execute a copy of this code.
(4)  You are free to do your own work.

*/

// copyright 2015 by Jack Coleman and Braydn Larsen