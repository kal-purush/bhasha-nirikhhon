const btnUpload = document.getElementById('myButton');
const divOutput = document.getElementById('divOutput');
const file = document.getElementById('myFile');

btnUpload.addEventListener('click', () => {
  // client API available to allow the user to select a File from his system.
  const fileReader = new FileReader();
  const theFile = file.files[0];

  // read the file as an array of bytes.
  fileReader.readAsArrayBuffer(theFile);

  // onload is triggered when the file loading from the device to the
  // chrome buffer is successful
  fileReader.onload = async (ev) => {
    console.log('File loaded successfully');

    const CHUNK_SIZE = 1000000; // 1 MB
    totalChunksAvailable = ev.total / CHUNK_SIZE;

    // create a unique filename to identify the chunks for the same file.
    const fileName = Math.random() * 1000 + theFile.name;
    for (let chunkId = 0; chunkId < totalChunksAvailable; chunkId++) {
      const start = chunkId * CHUNK_SIZE;
      const end = Math.min(chunkId * CHUNK_SIZE + CHUNK_SIZE, ev.total);
      const chunk = ev.target.result.slice(start, end);
      await fetch('/upload', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/octet-stream',
          'Content-Length': chunk.length,
          'file-name': fileName,
        },
        body: chunk,
      });
      divOutput.textContent = Math.round((end * 100) / ev.total, 0) + '%';
    }
  };
});