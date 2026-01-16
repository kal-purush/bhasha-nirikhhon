using System.Collections.Generic;
using System.Linq;

namespace LoginAPI.Models
{
    public class StateBasedAuth
    {
        public class Transition
        {
            public LoginState From { get; set; }
            public LoginState To { get; set; }
            public LoginTrigger Trigger { get; set; }

            public Transition(LoginState from, LoginState to, LoginTrigger trigger)
            {
                From = from;
                To = to;
                Trigger = trigger;
            }
        }

        public static readonly List<Transition> transitions = new()
        {
            new Transition(LoginState.Awal, LoginState.Validasi, LoginTrigger.Submit),
            new Transition(LoginState.Validasi, LoginState.Berhasil, LoginTrigger.Valid),
            new Transition(LoginState.Validasi, LoginState.Gagal, LoginTrigger.Invalid),
        };

        public static LoginState GetNextState(LoginState currentState, LoginTrigger trigger)
        {
            var transition = transitions.FirstOrDefault(t => t.From == currentState && t.Trigger == trigger);
            return transition != null ? transition.To : currentState;
        }
    }
}