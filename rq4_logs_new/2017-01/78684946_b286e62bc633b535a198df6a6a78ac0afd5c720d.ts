 Timeout() {
    if (this.stateService.GetCurrentState() != 'sleep' && this.stateService.GetCurrentState() != 'arm') {
      let object = this;
      object.timerAlarm = 5;
      object.AlarmSirene(50, 100);
      (function myLoop(timer) {
        setTimeout(function () {
          object.state = object.stateService.GetCurrentState();
          if (object.timerAlarm == 1 && object.stateService.GetCurrentState() == 'alarm') {
            object.stateService.FireAction("timeout");
            object.colour = "white";
            object.state = object.stateService.GetCurrentState();
            object.GetCurrentState();
          }
          if (object.stateService.GetCurrentState() == 'sleep') {
            object.timerAlarm = 1;
            object.state = object.stateService.GetCurrentState();
            object.colour = "white";
          }

          if (--object.timerAlarm) myLoop(object.timerAlarm); //  decrement i and call myLoop again if i > 0
        }, 1000)
      })(object.timerAlarm);
    } else {
      console.log('Alarm is already timed out');
    }
  }

  Alarm(timer,speed,soundspeed,soundnumber) {
    this.state = this.stateService.GetCurrentState();
    let object = this;
    object.timer = timer;
    object.AlarmSound(timer, soundspeed);
    (function myLoop(timer) {
      setTimeout(function () {
        object.state = object.stateService.GetCurrentState();
        console.log("Timer:", object.timer)
        if (object.timer == 1) {
            if (object.stateService.GetCurrentState() == 'alarm') {
                object.stateService.FireAction("timeout");
                object.colour = "white";
                object.state = object.stateService.GetCurrentState();
                object.GetCurrentState();
            }
            if(object.stateService.GetCurrentState() == 'arm')
                object.stateService.FireAction("intrusion");
                object.colour = "red"
                object.state = object.stateService.GetCurrentState();
                object.Alarm(5,1000,100,50)
        }
        if (object.stateService.GetCurrentState() == 'sleep') {
          object.timer = 1;
          object.colour = "white";
          object.state = object.stateService.GetCurrentState();
        } //  your code here                
        if (--object.timer) myLoop(object.timer); //  decrement i and call myLoop again if i > 0
      }, speed)
    })(object.timer),
    function () {};
  }