import { ActionTree } from "vuex"
import { RootState } from "@/store";
import { DBState } from "./state";
const low = window.require('lowdb')
const FileSync = window.require('lowdb/adapters/FileSync')
const actions: ActionTree<DBState, RootState> = {
    setDB({ state, commit }){        
        const adapter = new FileSync(state.path);
        const db = low(adapter);
        
        db.defaults({
            global:{},
            videos: []
        }).write();

        commit("SET_DB", db);
    },
}

export default actions;