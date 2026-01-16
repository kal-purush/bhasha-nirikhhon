import {useState} from 'react'
import { db,storage } from '../../firebase';
import { getDocs,collection, query, where, updateDoc, doc } from 'firebase/firestore';
import { getStorage, ref, uploadBytesResumable,getDownloadURL } from "firebase/storage";
import Compressor from 'compressorjs';
import './ProfileSetter.css';

const ProfileSetter = (props) => {
    const[image,setImage]=useState(null);
    const[progress,setProgress]=useState(0);
    const[caption,setCaption]=useState('');
    const [uploading, setUploading] = useState(false);
    
    const handleChange = (e) => {
        if (e.target.files[0]) {
          new Compressor(e.target.files[0], {
            quality: 0.8, 
            success: (compressedImage) => {
              setImage(compressedImage);
            },
            error: (error) => {
              console.log(error);
              alert(error);
            },
          });
        }
      };
  const handleUpload = () => {
    if (!image) {
        console.log("No image selected");
        return;
      }
    setUploading(true);
    const storageRef = ref(getStorage(), `images/${image.name}`);
    const uploadTask = uploadBytesResumable(storageRef, image, {
        chunkSize: 1 * 1024 * 1024 // 1MB chunks
      });

    uploadTask.on(
      "state_changed",
      (snapshot) => {
        const progress = Math.round(
          (snapshot.bytesTransferred / snapshot.totalBytes) * 100
        );
        setProgress(progress);
      },
      (error) => {
        console.log(error);
        alert(error);
      },
      async () => {
        try {
            const url = await getDownloadURL(uploadTask.snapshot.ref);
            const pRef = collection(db, 'login');
            const q = query(pRef, where('username', '==', props.user));
            const querySnapshot = await getDocs(q);
          
            querySnapshot.forEach(async (d) => {
              console.log(d.data())
              const docRef = doc(db, 'login', d.id);
          
              try {
                await updateDoc(docRef, {
                  profileURL: url
                });
                console.log("Updated Successfully");
              } catch (updateError) {
                console.error("Error updating document: ", updateError);
              }
            });
            setUploading(false);
          
          } catch (error) {
            console.error("Error getting download URL: ", error);
          }
          
      }
    );
  };
    
    return ( 
        <div className="ImageUploader">
            <p>Upload Your Profile</p>
            <hr />
            <input type="file" onChange={handleChange} className='choosefile' />
            <label htmlFor="choosefile" className='choosefile-label'></label>
            <progress value={progress} max="100"/>
            <button onClick={handleUpload} disabled={uploading} className='upload'>{uploading ? 'Uploading...' : 'Upload'}</button>
        </div>
     );
}
 
export default ProfileSetter;