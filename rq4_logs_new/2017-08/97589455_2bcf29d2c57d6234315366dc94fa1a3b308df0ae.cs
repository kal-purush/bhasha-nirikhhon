using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

public class Commands : Dispatcher {
    public static Commands Instance {
        get {
            if (smInstance == null) {
                smInstance = new Commands();
            }
            return smInstance;
        }
    }
    public static Commands smInstance;

    public void Bind(Cmd.SERVER_COMMAND id, Action<object> callback) {
        Bind((int)id, callback);
    }
    public void Trigger(Cmd.SERVER_COMMAND id, object args) {
        Trigger((int)id, args);
    }
}