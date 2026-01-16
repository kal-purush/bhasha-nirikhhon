const fetch = require("node-fetch");
const Discord = require('discord.js');


module.exports = {
  name: 'todo',
  description: 'adds a task to the todo list',
  execute(message, args) {
    // given args array, split it into two variables for the strings and the number at the end. So it should form a sentance followed by a number.
    // Example: ["finish", "biology", "project", "1"] -> task = "finish biology project" and number = 1
    // Example: ["finish", "biology", "project"] -> task = "finish biology project" and number = null
    // Example: ["finish", "biology", "project", "12"] -> task = "finish biology project" and number = 12
    // Example: ["finish", "biology", "project", "12", "13"] -> task = "finish biology project" and number = 12
    
    task = args.join(" ");

    // send a message to the channel that says "how long would you like to spend on this task? (in minutes)"
    // store the users response in a variable called time
    const embed = new Discord.MessageEmbed()
      .setColor('#0099ff')
      .setTitle('How long would you like to spend on this task?')
      .setDescription('Please enter a number in minutes.')
      .setFooter('Example: "30"');
    message.channel.send(embed);

    // store the users response in a variable called time
    const filter = m => m.author.id === message.author.id;
    message.channel.awaitMessages(filter, { max: 1, time: 60000, errors: ['time'] })
      .then(collected => {
        const time = collected.first().content;
        // send a message to the channel that says "Good luck completing your task, {task}! A timer for {time} minutes has been started."
        message.channel.send(`Good luck completing your task, ${task}! A timer for ${time} minutes has started. ⏳`);
        
        // when the timer is half way done, send a message to the channel that says "@user, you are half way done! Keep going 🤍"
        setTimeout(function() {
          message.channel.send(`${message.author}, you are half way done! Keep going 🤍`);
        }, time * 60000 / 2);
        // when the timer runs out, send a message to the channel that says "The timer is up! Great job!"
        setTimeout(function() {
          message.channel.send(`Congrats ${message.author}! You finished your task! Enjoy a quick break! 🥳`);
          
        }
        , time * 60000);
      })
    
    
      /*


    //const task = args[0];

    // start a timer for the task. It should last for the time specified in minutes. After the time is up, send a message that says good job!
    // if the task is not specified, send a message that says you need to specify a task.
    // if the time is not specified, send a message that says you need to specify a time.
    // if the time is not a number, send a message that says you need to specify a number.
    // if the time is not a number between 1 and 60, send a message that says you need to specify a number between 1 and 60.
    if (task === undefined) {
      message.channel.send('You need to specify a task!');
    }
    if (time === undefined) {
      message.channel.send('You need to specify a time!');
    }
    if (time === NaN) {
      message.channel.send('You need to specify a number!');
    }
    if (time < 1 || time > 60) {
      message.channel.send('You need to specify a number between 1 and 60!');
    }
    // if the time is a number, send a message that says @user good job!
    const timer = setTimeout(() => {
      message.channel.send(`${message.author} good job!`);
    }, time * 60000);
    
    message.channel.send(`${task} has been added to the todo list!`);

*/

  }


}
