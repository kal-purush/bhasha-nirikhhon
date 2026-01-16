import React, { useState } from 'react';
import axios from 'axios';

function App() {
  const [selectedImage, setSelectedImage] = useState(null);
  const [cleanedText, setCleanedText] = useState('');
  const [error, setError] = useState(null);

  const handleImageUpload = async () => {
    const formData = new FormData();
    formData.append('image', selectedImage);

    try {
      const response = await axios.post('http://localhost:5000', formData);
      const cleanedText = response.data;
      setCleanedText(cleanedText);
      setError(null);
    } catch (error) {
      setCleanedText('');
      setError('Error processing the image.');
    }
  };

  return (
    <div className='bg-gray-100'>
      {/* Navigation Bar */}
      <nav className='bg-blue-500 p-4'>
        <div className='container mx-auto flex justify-between'>
          <h1 className='text-white text-3xl font-bold'>Image to Text App</h1>
          <a href="mailto:uabdureman@gmail.com" target='_blank' rel='noopener noreferrer' className='text-white bg-blue-700 p-2 rounded'>
            <button>Hire Me</button>
          </a>
        </div>
      </nav>

      {/* Main Content */}
      <div className='container mx-auto p-6'>
        {/* Welcome Message */}
        <div className='text-center mt-10'>
          <h2 className='text-3xl font-bold text-blue-500 mb-4'>Welcome to our Image to Text converter App</h2>
          <p className='text-gray-600'>
            Easily extract text from images and documents. Upload an image below to get started.
          </p>
        </div>

        <div className='grid grid-cols-1 gap-4 md:grid-cols-2'>
          {/* Image Upload */}
          <div className='text-center'>
            <input
              type='file'
              accept='image/*'
              onChange={(e) => setSelectedImage(e.target.files[0])}
            />
            <button
              className='bg-blue-500 hover:bg-blue-700 text-white font-bold py-2 px-4 rounded mt-4'
              onClick={handleImageUpload}>
              Submit
            </button>
          </div>
 <div>
  
 </div>
          {/* Image Preview */}
          {selectedImage && (
            <div className='text-center'>
              <h1 className='text-gray-800 mt-4'>Uploaded Image</h1>
              <img
                src={URL.createObjectURL(selectedImage)}
                alt='Uploaded'
                className='w-full max-w-lg mt-4'
              />
            </div>
          )}

          {/* Display Cleaned Text */}
          <div className='text-center'>
            <h1 className='text-gray-800 mt-4'>Extracted Text</h1>
            <textarea
              className='border-2 p-2 w-full h-64'
              value={cleanedText}
              onChange={(e) => setCleanedText(e.target.value)}
              placeholder='Extracted text will appear here'
            />
          </div>
        </div>

        {/* Error Message */}
        {error && (
          <div className='text-red-500 mt-4 text-center'>
            {error}
          </div>
        )}
      </div>

      {/* Footer */}
      <footer className='bg-gray-900 text-white p-8 mt-10'>
        <div className='mx-auto text-center'>
          &copy; {new Date().getFullYear()} Image to Text App
        </div>
        <div className='mx-auto text-center'>
          <h1>Developed by <a href='https://github.com/Usmaelabdureman' rel='noopener noreferrer' className='text-yellow-500' target='_blank'>Usmael</a></h1>
        </div>
      </footer>
    </div>
  );
}

export default App;