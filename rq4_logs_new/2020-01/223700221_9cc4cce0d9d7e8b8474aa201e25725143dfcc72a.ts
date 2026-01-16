import { RootState } from "@/store";
import { ActionTree } from "vuex";
const { remote } = window.require("electron");
const fs = window.require("fs");
const path = window.require("path");

const actions: ActionTree<RootState, RootState> = {
    async setVideos({state, commit}){
      
        const files = fs.readdirSync(state.videos.path);
        commit("SET_VIDEOS", files);      
    },
    async setCurrentVideo({state, commit, dispatch}, fileName: string){
        commit("SET_CURRENT_VIDEO", `${state.videos.path}/${fileName}`);
        dispatch("kde/setWallpaperVideo", {root: true});
    },
    async addVideo({state, dispatch}){
        
        const options = { 
            properties: [ 'openFile', 'multiSelections']        
        };

        const file = await remote.dialog.showOpenDialog(options);
        
        if(file.canceled){
            return
        }

        const filePath = file.filePaths[0];
        const fileName = path.basename(filePath);
        
        const copy = fs.copyFileSync(filePath, `${state.videos.path}/${fileName}`);
        
        if(copy){
            throw new Error(copy);
        }

        dispatch("setVideos");
    
    },
    async deleteVideo({state, dispatch}, fileName: string){      
    
        const filePath = `${state.videos.path}/${fileName}`;
        
        const deleteFile = fs.unlinkSync(filePath);
        
        if(deleteFile){
            console.log(fileName);
        }

        dispatch("setVideos");

    }      
}

export default actions;