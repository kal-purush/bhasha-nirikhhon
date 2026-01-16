export type Slide = {
    title: string
    date1: string
    date2: string
    date3: string
}

export const entrance: Slide[] = [
    {title: 'TOHOFES 70th', date1: '６月５日（土曜日）', date2: '６月６日（日曜日）', date3: '６月７日（月曜日）',}
]

export type ContentInfo = {
    title: string
    detail: string
}

export const info1: ContentInfo[] = [
    {title:'委員長挨拶', detail:'委員長たちの面白い挨拶',}
]
export const info2: ContentInfo[] = [
    {title:'開催日時', detail:'最高の３日間',}
]
export const info3: ContentInfo[] = [
    {title:'最新情報', detail:'随時更新中！',}
]
export const info4: ContentInfo[] = [
    {title:'パンフレット', detail:'こだわりの１冊',}
]