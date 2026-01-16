export const BASE_URL = 'https://hacker-news.firebaseio.com/v0';
export const TOP_STORIES_IDS = '/topstories.json?print=pretty';
export const STORIES_DETAIL = (id: number) =>  `/item/${id}.json`;

export const SERVER_REQUEST = async (url: string, type: string, body: any = null) => {
    try {
        const response = type.toLowerCase() === 'post' ?
            await fetch(`${BASE_URL}${url}`,
                {
                    method: 'POST',
                    headers: {
                     'Content-Type': 'application/json'
                    },
                    body: JSON.stringify(body)
                }
            )
                : await fetch(`${BASE_URL}${url}`,
                {
                    method: 'GET',
                    headers: {
                    'Content-Type': 'application/json'
                    }
                }
                )
            const data = await response.json();
        return data;
    } catch (error){
        return error;
    }
}