import fetch from 'utils/fetch'
import { deleteEmptyProperty } from 'utils/filter'
import { deepCoyp } from 'utils/copy'

export function appList(search, page, size) {
    let searchCopy = deepCoyp(search)
    deleteEmptyProperty(searchCopy)

    let params = new FormData()
    params.append('search', JSON.stringify(searchCopy))
    params.append('page', page)
    params.append('size', size)
    return fetch({
        url: '/xqh/ad/app/search',
        method: 'post',
        data: params,
        headers: {'Content-Type': 'application/x-www-form-urlencoded'}
    })
}

export function appListNoPage() {
    const params = new FormData()
    params.append('search', '{}')
    params.append('page', 1)
    params.append('size', 1000)
    return fetch({
        url: 'xqh/ad/app/search',
        method: 'post',
        data: params,
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' }
    })
}

export function appCreate(body) {
    const bodyCopy = deepCoyp(body)
    return fetch({
        url: 'xqh/ad/app',
        method: 'put',
        data: bodyCopy
    })
}

export function appUpdate(body) {
    const bodyCopy = deepCoyp(body)
    return fetch({
        url: 'xqh/ad/app',
        method: 'post',
        data: bodyCopy
    })
}