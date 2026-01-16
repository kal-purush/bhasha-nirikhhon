import crypto from "crypto"
const unsignedUploadPreset = 'nlgprs4r'

const imageUpload = async (file: File, publicID: string) => {
    try {
        let url = ''
        let data = new FormData()
        data.append("file", file)
        data.append("public_id", publicID)
        data.append("upload_preset", unsignedUploadPreset)
        await fetch("https://api.cloudinary.com/v1_1/alp-portal/upload", {
            method: 'POST',
            body: data,
          })
            .then((response) => response.json())
            .then((data) => {
              // File uploaded successfully
              url = data.secure_url
            }
            )
        return url
    } 
    catch(e) {
        console.log("ERROR")
        console.log(e)
        return ``
    }
}
const generateSHA1 = (data: any) => {
    const hash = crypto.createHash("sha1");
    hash.update(data);
    return hash.digest("hex");
  };
  
  const generateSignature = (publicId: string, apiSecret: string | undefined, timestamp: string) => {
    return `public_id=${publicId}&timestamp=${timestamp}${apiSecret}`;
  };

const imageDelete = async (publicId : string) => {
    try {
        console.log("here")
        const cloudName = "alp-portal";
        const apiKey = "272711192725652";
        const apiSecret = "6X7Fj1EPJa2YqOgGqiE7mvL5Ras";  
        const timestamp = new Date().getTime().toString();
        const signature = generateSHA1(generateSignature(publicId, apiSecret, timestamp)).toString();
        const url = `https://api.cloudinary.com/v1_1/${cloudName}/image/destroy`;
        let data = new FormData()
        data.append("public_id", publicId)
        data.append("signature", signature)
        data.append("api_key", apiKey)
        data.append("timestamp", timestamp.toString())
        await fetch(url, {
            method: 'POST',
            body: data
        }).then((response) => response.json()).then((data) => console.log(data))
    }
    catch(e) {
        console.log("ERROR")
        console.log(e)
        return ``
    }
}
export {imageUpload, imageDelete}