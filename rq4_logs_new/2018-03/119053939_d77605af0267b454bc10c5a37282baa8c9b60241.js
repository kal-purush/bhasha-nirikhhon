import face from './FaceApi';

const detectFace = ({
    url,
}) => {
    const requestUrl = '/face/v1.0/detect';
    const body = JSON.stringify({ url });

    return face.post(requestUrl, body);
}

export {
    detectFace,
}