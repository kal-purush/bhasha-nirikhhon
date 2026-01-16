const mongoDbData = (collection: string) => {
    return JSON.stringify({
        collection: collection,
        dataSource: 'PantriiApp',
        database: 'pantriiapp',
    });
};

export const mongoDbConfig = (method: string, collection: string) => {
    return {
        method: method,
        url: 'https://data.mongodb-api.com/app/data-oxvtw/endpoint/data/beta/action/find',
        headers: {
            'Content-Type': 'application/json',
            'Access-Control-Request-Headers': '*',
            'Api-key':
                '2GZQT0hpTykckgCnk5ajds55663JisDpmQg3r9gy94YhgO9rDay9NeEzClKm6jcc',
        },
        data: mongoDbData(collection),
    };
};