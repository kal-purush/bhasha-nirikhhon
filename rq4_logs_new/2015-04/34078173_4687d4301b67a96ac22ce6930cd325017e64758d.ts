module StateMachine {
    
    // ------------------------------------------------------------------
    class Transition {
        public trigger: string;
        srcState: string;
        tgtState: string;
        
        constructor(trigger: string, src: string, tgt: string) {
            this.trigger = trigger;
            this.srcState = src;
            this.tgtState = tgt;
        }

        public TriggerMatch(t: string): boolean {
            return this.trigger == t
        }

        public SourceMatch(src: string): boolean {
            return this.srcState == src
        }
    }
    
    // ------------------------------------------------------------------
    export class StateMachine {
        private curState: string;
        private transitions: Transition[];
        private errorState: boolean;
        private errorMsg: string;
        
        constructor() {
            this.curState = "cur-state-not-initialized"
            this.transitions = [];
            this.errorState = false;
            this.errorMsg = "no-error";
        }

        public AddStart(s: string) {
            this.curState = s;
        }
        
        public AddTransition(trigger: string, src: string, tgt: string) {
            var t = new Transition(trigger, src, tgt);
            this.transitions.push(t);
        }
        
        public Trigger(trigger: string) {
            if (!this.ContainsTrigger(trigger)) {
                this.errorState = true;
                this.errorMsg = "StateMachine: Couldn't find trigger: " + trigger;
                return;
            }
            
            for (var i in this.transitions) {
                var t = this.transitions[i]                
                if (t.TriggerMatch(trigger)) {
                    if (t.SourceMatch(this.curState)) {
                        console.log(t);
                        this.curState = t.tgtState;
                        return;
                    } 
                }
            }
            this.errorState = true;
        }
        
        public ContainsState(s: string): boolean {
            for (var idx in this.transitions) {
                var t = this.transitions[idx];
                console.log(t)
                if (s == t.srcState || s == t.tgtState) {
                    return true;
                }
            }            
            return false;
        }

        public ContainsTrigger(trigger: string): boolean {
            for (var idx in this.transitions) {
                var t = this.transitions[idx];
                console.log(t)
                if (trigger == t.trigger) {
                    return true;
                }
            }            
            return false;
        }
        
        public GetCurrentState(): string {
            return this.curState;
        }

        public IsDead(): boolean {
            return this.errorState;
        }

        public StateIs(s: string): boolean {
            return this.GetCurrentState() == s;
        }
    }
}