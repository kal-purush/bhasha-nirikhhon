var apiRequest = axios.create({
    baseURL: `http://127.0.0.1:5500/reference`
});

var apiGetTagPostJson = function (data) {
    return apiRequest.get(`/TagPostRelationship.json`);
};


