import request from '@/frame_src/utils/request'

export function GetCodeOptions(query) {
    return request({
        url: '/DictionaryConfig/GetCodeOptions',
        method: 'get',
        params: query
    })
}

export function GetCRKInfo(query) {
    return request({
        url: '/CRKWH/GetCRKInfo',
        method: 'get',
        params: query
    })
}

export function CreateCRKInfo(data) {
    return request({
        url: '/CRKWH/CreateCRKInfo',
        method: 'post',
        data
    })
}

export function UpdateCRKInfo(data) {
    return request({
        url: '/CRKWH/UpdateCRKInfo',
        method: 'post',
        data
    })
}

export function DelCRKInfo(data) {
    return request({
        url: '/CRKWH/DelCRKInfo',
        method: 'post',
        data
    })
}