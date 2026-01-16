var pomodoro = {
    started: false,
    status: "session",
    interval: null,
    break: {
        len : 5,
        displayDom : null
    },
    session: {
        len : 25,
        displayDom: null
    },
    // counter variables
    hrs: 0,
    mins: 25,
    secs: 0,
    hrDom: null,
    minDom: null,
    secDom: null,
    statusDom: null,
    timeDom: null,

    // filler variables
    fillerHeight: 0,
    fillerIncr: 0,
    fillerDom: null,

    // methods
    // format time display, always show 2 digits
    format: function(t) {
      return ("0"+t).slice(-2);
    },

    // convert len to hr/min/sec
    len2hms: function(len) {
      var h = Math.floor(len / 60);
      this.hrs = h > 0 ? h : 0;
      this.mins = len % 60;
      this.secs = 0;
    },

    updateTimerDom: function() {
        this.hrDom.textContent = (this.hrs > 0 ? (this.format(this.hrs)+":") : "");
        this.minDom.textContent = this.format(this.mins);
        this.secDom.textContent = this.format(this.secs);
    },

    toggleStatus: function() {
        if (this.status === "session") {
            this.status = "break";
        } else {
            this.status = "session";
        }
    },

    updateStatusDom: function() {
        this.statusDom.textContent = this.status;
    },

    adjustLen: function(status, step) {
        if (this.started) {
            return;
        }
        else {
            var len = this[status].len;
            if (len > 1 && len < 1440) {
                this[status].len += step;
                this[status].displayDom.textContent = this[status].len;
            }
            if (this.status === status) {
                this.len2hms(this[status].len);
                this.updateTimerDom();
                this.resetFiller();
            }
        }
    },

    resetFiller: function() {
      this.fillerIncrement = 300/(this[this.status].len*60);
      this.fillerHeight = 0;
      this.fillerDom.style.backgroundColor
        = (this.status === "session") ? "#99CC00" : "#FF4444";
    },

    increFiller: function() {
        this.fillerHeight = this.fillerHeight + this.fillerIncrement;
    },

    updateFillerDom: function() {
        this.fillerDom.style.height = this.fillerHeight + 'px';
    },

    countDown:function() {
      if (this.secs == 0) {
        if (this.mins == 0) {
          if (this.hrs == 0) {
            this.toggleStatus();
            this.updateStatusDom();
            this.len2hms(this[this.status].len);
            this.updateTimerDom();
            this.resetFiller();
            this.updateFillerDom();
            return;
          } else {
            --this.hrs;
            this.mins = 59;
            this.secs = 59;
          }
        } else {
          --this.mins;
          this.secs = 59;
        }
      } else {
        --this.secs;
      }
      this.updateTimerDom();
      this.increFiller();
      this.updateFillerDom();
    },

    init: function() {
        var self = this;
        this.break.displayDom = document.getElementById("break");
        this.session.displayDom = document.getElementById("session");
        this.hrDom = document.getElementById("hours");
        this.minDom = document.getElementById("minutes");
        this.secDom = document.getElementById("seconds");
        this.statusDom = document.getElementById("status");
        this.timeDom = document.getElementById("time");
        this.fillerDom = document.getElementById("filler");

        this.resetFiller();

        // attach event handler for buttons
        /*
        var buttons = Array.from(document.getElementsByTagName("button"));
        for (var i = 0; i < buttons.length; i++) {
            var status = buttons[i].parentElement.className;
            var step = buttons[i].className === "minus" ? -1 : 1;
            buttons[i].onclick = this.adjustLen.bind(self, status, step);
        }
        */
        Array.from(document.getElementsByTagName("button")).forEach(function(obj) {
            var status = obj.parentElement.className;
            var step = obj.className === "minus" ? -1 : 1;
            obj.onclick = this.adjustLen.bind(this,status, step);
        }, self);

        this.timeDom.onclick = function() {
          self.started = !self.started;
          if (!self.started) {
            if (!self.interval) return;
            clearInterval(self.interval);
            self.interval = null;
          } else {
            self.interval = setInterval(function() {
              self.countDown.apply(self);
            }, 1000);
          }
        }
    },
}

window.onload = function() {
    pomodoro.init();
};