import express from 'express';
import musicMetadata from 'music-metadata';
import recursive from 'recursive-readdir';

const app = express();
const PORT = 1486;
const mp3Folder = process.cwd(); // use the application's directory

app.use(express.static(mp3Folder)); // serve mp3 files

// Redirect from the root path to '/songs'
app.get('/', (req, res) => {
  res.redirect('/songs');
});

app.get('/songs', async (req, res) => {
  const mp3Files = [];

  // Read all mp3 files from the directory and its subdirectories
  const files = await recursive(mp3Folder, ['!*.mp3']);

  // Get metadata for each mp3 file
// Get metadata for each mp3 file
for (let file of files) {
  const metadata = await musicMetadata.parseFile(file);
  let title = metadata.common.title;
  if (!title) {
    title = file.split('/').pop(); // Use filename as title if title doesn't exist in metadata
  }
  mp3Files.push({
    title: title,
    artist: metadata.common.artist,
    file: file.replace(mp3Folder, ''),
  });
}

// Sort mp3 files alphabetically
mp3Files.sort((a, b) => a.title.localeCompare(b.title));


  res.json(mp3Files);
});

app.listen(PORT, () => console.log(`Server started on port ${PORT}`));