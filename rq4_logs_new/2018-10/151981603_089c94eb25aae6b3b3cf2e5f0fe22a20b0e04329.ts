const helper = {
    getBase64(img: any, callback: any) {
        const reader = new FileReader();
        reader.readAsDataURL(img);
        reader.addEventListener('load', () => callback(reader.result));
    }
};

export default helper;