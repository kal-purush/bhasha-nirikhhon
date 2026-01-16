import request from "@/utils/request";
import { praseStrEmpty } from "@/utils/ruoyi";

// 查询直播间列表
export function listLive(query) {
    return request({
        url: "/admin/live/lives",
        method: "get",
        params: query
    });
}

// 查询直播间详细
export function getLive(liveId) {
    return request({
        url: "/admin/live/lives/" + praseStrEmpty(liveId),
        method: "get"
    });
}

// 新增直播间
export function addLive(data) {
    return request({
        url: "/admin/live/lives/add",
        method: "post",
        data: data
    });
}


// 修改直播间信息
export function updateLive(data) {
    return request({
        url: "/admin/live/lives/update",
        method: "post",
        data: data
    });
}

// 删除直播间
export function delLive(data) {
    return request({
      url: "/admin/live/lives/del",
      method: "post",
      data: data
    });
  }

// 所有模块
export function allTag() {
    return request({
        url: "/admin/live/tag/list",
        method: 'get'
    })
}