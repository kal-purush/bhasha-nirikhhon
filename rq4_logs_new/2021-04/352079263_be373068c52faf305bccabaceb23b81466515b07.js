const TEXTS = {
   main: [
      "Greetings. I'm Vidush H. Namah 🐱‍💻",
      "I'm a software craftsman.",
      "I make stuff in my free time.",
      "I'm a design and tech enthusiast.",
      "Stick around for the ride 🐱‍🐉"
   ],
   sub: [
      "Welcome to my playground.",
      "Full Stack with a bit of spice.",
      "Arduino, Python, ESP8266/32, Raspberry Pi.",
      "A blank file hath potential deep as a white canvas",
      "Follow me on Twitter: @VHNamah"
   ]
}

const TIMINGS = {
   entry: 750,
   static: 2500,
   exit: 750
}

class Messenger {
   constructor(el, texts, time) {
      'use strict';
      var m = this;

      m.start = function (delay = 100) {
         setTimeout(function() {
            m.animateIn()
         }, delay);
      };

      m.init = function (watch) {
         m.codeletters = "&#*+%?£@§$";
         m.message = 0;
         m.current_length = 0;
         m.fadeBuffer = false;
         m.leftover = "";
         m.messages = texts;
         m.watch = watch;
         m.complete = false;
      };

      m.animateInTimeSplit = function () {
         const x = (time.entry / (m.messages[m.message].length - m.leftover.length));
         return x;
      };

      m.animateOutTimeSplit = function () {
         const x = (time.exit / (m.messages[m.message].length - m.leftover.length));
         return x;
      };

      m.generateRandomString = function (length) {
         var random_text = '';
         while (random_text.length < length) {
            random_text += m.codeletters.charAt(Math.floor(Math.random() * m.codeletters.length));
         }

         return random_text;
      };

      m.animateIn = function () {
         m.complete = false;
         $('.cursor').show();

         if (m.current_length < m.messages[m.message].length) {
            m.current_length = m.current_length + 2;
            if (m.current_length > m.messages[m.message].length) {
               m.current_length = m.messages[m.message].length;
            }

            var message = m.leftover + m.generateRandomString(m.current_length - m.leftover.length);
            $(el).html(message);

            setTimeout(m.animateIn, m.animateInTimeSplit());
         } else {
            setTimeout(m.animateFadeBuffer, m.animateInTimeSplit());
         }
      };

      m.animateOut = function () {
         var msg = m.messages[m.message];
         var nextMsgIndex = m.message + 1;
         if (nextMsgIndex < m.messages.length) {

            var nextMsg = m.messages[nextMsgIndex];
            var trimmedMsg = msg.substr(0, m.current_length);
            var trimmedNextMsg = nextMsg.substr(0, m.current_length);

            if (trimmedMsg != trimmedNextMsg && m.current_length > 0) {
               m.current_length = m.current_length - 1;
               var message = msg.substr(0, m.current_length);

               $(el).html(message);
               setTimeout(m.animateOut, m.animateOutTimeSplit());
            } else {
               $('.cursor').hide();

               m.leftover = trimmedMsg;
               m.cycleText();
            }
         }
      };

      m.animateFadeBuffer = function () {
         if (m.fadeBuffer === false) {
            m.fadeBuffer = [];
            for (var i = m.leftover.length; i < m.messages[m.message].length; i++) {
               m.fadeBuffer.push({ c: (Math.floor(Math.random() * 12)) + 1, l: m.messages[m.message].charAt(i) });
            }
         }

         var do_cycles = false;
         var message = m.leftover;

         for (var i = 0; i < m.fadeBuffer.length; i++) {
            var fader = m.fadeBuffer[i];
            if (fader.c > 0) {
               do_cycles = true;
               fader.c--;
               message += m.codeletters.charAt(Math.floor(Math.random() * m.codeletters.length));
            } else {
               message += fader.l;
            }
         }

         $(el).html(message);

         if (do_cycles === true) {
            setTimeout(m.animateFadeBuffer, m.animateInTimeSplit());
         } else {
            setTimeout(m.animateOut, time.static);
         }
      };

      m.cycleText = function () {
         m.message = m.message + 1;
         if (m.message >= m.messages.length) {
            m.message = 0;
         }

         //m.current_length = 0;
         m.fadeBuffer = false;
         m.complete = true;
         //$(el).html('');
         if (m.watch.complete) {
            setTimeout(m.animateIn, 200);
            setTimeout(m.watch.animateIn, 200);
         }
      };
   }
}

$(document).ready(function () {
   const textAnimation = new Messenger($('#main'), TEXTS.main, TIMINGS);
   const subtextAnimation = new Messenger($('#sub'), TEXTS.sub, TIMINGS);

   $('.cursor').hide();

   textAnimation.init(subtextAnimation);
   subtextAnimation.init(textAnimation);

   textAnimation.start(1000);
   subtextAnimation.start(975);
});