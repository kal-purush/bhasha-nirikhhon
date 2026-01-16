

import {StateManager, State, Server} from "./types"

// todo: Replace with PouchDB
export class DefaultStateManager implements StateManager {

  state: State = {} 

  set(key: string, value: State): State {
    this.state[key] = value;
    return this.state[key];
  }  
  get(key: string): State {
    return this.state[key] || {}
  }

  mount(server: Server) {
    server.get("/state/:module", (req, res) => {
      let {module} = req.params;
      res.send(this.get(module));
    });

    server.put("/state/:module", (req, res) => {
      let {module} = req.params;
      res.send(this.set(module, req.body));
    })
  }
}