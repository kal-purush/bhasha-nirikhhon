import axios from 'axios'

export interface BookData {
    meta: {
        total_count: number
        pageable_count: number
        is_end: boolean
    }
    documents: {
        title: string
        contents: string
        url: string
        isbn: string
        datetime: string
        authors: string[]
        publisher: string
        translators: string[]
        price: number
        sale_price: number
        thumbnail: string
        status: string
    }[]
}

const getSearchBook = async (query: string): Promise<BookData> => {
    const response = await axios.get<BookData>(
        'https://dapi.kakao.com/v3/search/book',
        {
            headers: {
                Authorization: `KakaoAK ${process.env.REACT_APP_KAKAO_REST_API_KEY}`,
            },
            params: {
                query,
            },
        }
    )

    return response.data
}

export default getSearchBook