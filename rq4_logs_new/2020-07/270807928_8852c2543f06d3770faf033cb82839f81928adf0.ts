import { imageBucket } from ".";



export async function saveProfilePicture(contentType:string, imageBase64Data:string, fileName:string){
    try{

        let newImage = imageBucket.file(fileName)// make a new file
        
        //Buffer.from will give us a binary array from the arguements we supply
        //file.save function needs binary data ( this is because it streams binary data)
        await newImage.save(Buffer.from(imageBase64Data, 'base64'), {//try to save the data to that file
            metadata:{
                contentType//set some metadata about the new file
            }
        })
        console.log('post file save')
    } catch(e){
        console.log(e);
        throw e  
    }
   

}