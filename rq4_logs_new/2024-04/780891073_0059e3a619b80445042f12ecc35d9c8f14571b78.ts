import { Status } from './bookType'

// Record의 태그(감상평, 인용, 줄거리, 의문점)
const tag = {
    review: 'review',
    quote: 'quote',
    summary: 'summary',
    question: 'question',
} as const

export type Tag = (typeof tag)[keyof typeof tag]

// 서재 상세 데이터 타입
export interface LibraryDetailType {
    status: Status
    rate: number | null
    startDate: string | null
    endDate: string | null
    page: number | null
    expectation: string | null
    recordList: RecordType[]
}

// 기록 데이터 타입
export interface RecordType {
    recordId: number
    page: number | null
    tag: Tag
    date: string
    content: string
}