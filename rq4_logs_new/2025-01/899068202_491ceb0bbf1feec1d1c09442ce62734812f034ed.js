const filestackClient = filestack.init('YOUR_API_KEY'); // Replace 'YOUR_API_KEY' with your actual Filestack API key.
    const uploadButton = document.getElementById('upload-button');
    const fileList = document.getElementById('file-list');
    const previewContainer = document.getElementById('preview-container');

    uploadButton.addEventListener('click', () => {
      filestackClient.picker({
        maxFiles: 5, // Allow up to 5 files
        accept: 'application/*', // Accept all document types
        onUploadDone: (result) => {
          fileList.innerHTML = ''; // Clear the file list before showing new files
          previewContainer.innerHTML = ''; // Clear previous previews

          result.filesUploaded.forEach(file => {
            // Display file name
            const fileNameElement = document.createElement('div');
            fileNameElement.classList.add('file-name');
            fileNameElement.innerText = file.filename;
            fileList.appendChild(fileNameElement);

            // Add preview iframe
            const iframe = document.createElement('iframe');
            iframe.src = file.url;
            previewContainer.appendChild(iframe);
          });

          // Success message
          const successMessage = document.createElement('div');
          successMessage.classList.add('success-message');
          successMessage.innerText = 'Files uploaded successfully!';
          fileList.appendChild(successMessage);
        }
      }).open();
    });