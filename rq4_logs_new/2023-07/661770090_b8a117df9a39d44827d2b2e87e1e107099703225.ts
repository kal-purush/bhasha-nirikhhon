import { getStorage, ref, listAll, getDownloadURL } from 'firebase/storage'
import { collection, addDoc } from 'firebase/firestore'
import { db } from './Firebase'

const fetchImagesFromPathAndSaveToFirestore = async (path : string) => {
    try {
      // Mendapatkan instance storage dari Firebase SDK
      const storage = getStorage()
      const storageRef = ref(storage, path)
  
      // Mengambil daftar file dari direktori storageRef
      const files = await listAll(storageRef)
  
      // Iterasi melalui setiap file
      for (const fileRef of files.items) {
        // Mendapatkan URL download untuk setiap file
        const downloadURL = await getDownloadURL(fileRef)
  
        // Konversi downloadURL menjadi string dan menyimpannya ke Firestore
        const imageUrl = downloadURL.toString()
        await addDoc(collection(db, 'Logo'), {
          url: imageUrl,
        })
  
        console.log('Image URL saved to Firestore:', imageUrl)
      }
  
      console.log('All image URLs saved to Firestore successfully.')
    } catch (error) {
      console.log('Error fetching images and saving URLs to Firestore:', error)
      throw error
    }
  }
  
  const path = 'gs://manadong-website.appspot.com/Menu'
fetchImagesFromPathAndSaveToFirestore(path)
  .then(() => {
    console.log('Image URLs fetched and saved successfully.')
    // Lakukan tindakan atau logika yang diinginkan setelah operasi berhasil.
  })
  .catch((error) => {
    console.log('Error fetching images and saving URLs:', error)
    // Tangani kesalahan yang terjadi saat mengambil URL gambar atau menyimpan URL ke Firestore.
  })