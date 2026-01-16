import { me } from "companion";
import { peerSocket } from "messaging";

var hrm_data = []

const ABNORMAL_WINDOW = 100;
const THRESHOLD = 500;

function sendMessage(obj){
    if(peerSocket.readyState === peerSocket.OPEN){
        peerSocket.send(obj);
        return true;
    }
    return false;
}

peerSocket.addEventListener("message", (obj) => {
    if(obj.status == "heart_rate"){
        hrm_data.push(obj.data);
        isAttack();
    }
});

function unusual(fm, fs, sm, ss){
    if(Math.abs(fm - fs) > 40 && Math.abs(sm - ss) > 10){
        return true;
    }
    return false;
}

function getStats(l, r){
    let res = {
        mean: 0,
        std_dev: 0
    };
    l = Math.max(l, 0);
    r = Math.min(r, hrm_data.length - 1);
    for(let i = l; i <= r; i++){
        mean += hrm_data[i];
    }
    mean /= (r - l + 1);
    for(let i = l; i <= r; i++){
        std_dev += (mean - hrm_data[i]) * (mean - hrm_data[i]);
    }
    std_dev /= (r - l + 1);
    return res;
}

function isAttack(){
    let len = hrm_data.length;
    if(len < THRESHOLD){
        return; //need at least 100 data points
    }
    let recent = getStats(len - ABNORMAL_WINDOW, len - 1);
    let old = getStats(0, len - ABNORMAL_WINDOW - 1);
    if(unusual(old.mean, old.std_dev, recent.mean, recent.std_dev)){
        sendMessage({
            status : "abnormal_cardiac_pattern",
            message : ""
        });
        hrm_data.splice(len - ABNORMAL_WINDOW, ABNORMAL_WINDOW); // don't include abnormal data in baseline
    }
}
