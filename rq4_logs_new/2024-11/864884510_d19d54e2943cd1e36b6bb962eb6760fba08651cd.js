const axios = require("axios");
const fs = require('fs-extra');
const { getStreamFromURL, shortenURL, randomString } = global.utils;

module.exports = {
  config: {
    name: "spotify",
    version: "1.7",
    author: "Vex_Kshitiz/coffee",
    countDown: 10,
    role: 0,
    shortDescription: "play song from spotify",
    longDescription: "play song from spotify",
    category: "music",
    guide: "{pn} sing songname OR {pn} songname by artist"
  },

  onStart: async function ({ api, event, args, message }) {
    try {
      // Set a reaction to indicate processing
      api.setMessageReaction("🕢", event.messageID, (err) => {
        if (err) console.error("Error setting reaction:", err);
      }, true);

      // Function to get song title and artist from command arguments
      const getSongTitleAndArtist = () => {
        let songTitle, artist;

        const byIndex = args.indexOf("by");
        if (byIndex !== -1 && byIndex > 0 && byIndex < args.length - 1) {
          songTitle = args.slice(0, byIndex).join(" ");
          artist = args.slice(byIndex + 1).join(" ");
        } else {
          songTitle = args.join(" ");
        }

        return { songTitle, artist };
      };

      // Determine song title and artist from command arguments
      let songTitle, artist;

      if (args.length === 0) {
        throw new Error("Please provide a song name.");
      } else {
        ({ songTitle, artist } = getSongTitleAndArtist());
      }

      // Array of services to fetch track URLs
      const services = [
        { url: 'https://spotify-play-iota.vercel.app/spotify', params: { query: songTitle } },
        { url: 'http://zcdsphapilist.replit.app/spotify', params: { q: songTitle } },
        { url: 'https://samirxpikachuio.onrender.com/spotifysearch', params: { q: songTitle } },
        { url: 'https://openapi-idk8.onrender.com/search-song', params: { song: songTitle } },
        { url: 'https://markdevs-last-api.onrender.com/search/spotify', params: { q: songTitle } }
      ];

      // Function to fetch track URLs from multiple services
      const fetchTrackURLs = async () => {
        for (const service of services) {
          try {
            const response = await axios.get(service.url, {
              params: service.params
            });

            if (response.data.trackURLs && response.data.trackURLs.length > 0) {
              console.log(`Track URLs fetched from ${service.url}`);
              return response.data.trackURLs;
            } else {
              console.log(`No track URLs found at ${service.url}`);
            }
          } catch (error) {
            console.error(`Error with ${service.url} API:`, error.message);
          }
        }

        throw new Error("No track URLs found from any API.");
      };

      // Fetch track URLs and select the first one
      const trackURLs = await fetchTrackURLs();
      const trackID = trackURLs[0];

      // Fetch download link for the selected track ID
      const downloadResponse = await axios.get(`https://sp-dl-bice.vercel.app/spotify?id=${encodeURIComponent(trackID)}`);
      const downloadLink = downloadResponse.data.download_link;

      // Download the track and send as a reply
      const filePath = await downloadTrack(downloadLink);
      await message.reply({
        body: `🎧 Playing: ${songTitle}${artist ? ` by ${artist}` : ''}`,
        attachment: fs.createReadStream(filePath)
      });

      // Delete the downloaded file after sending
      fs.unlink(filePath, (err) => {
        if (err) console.error("Error deleting file:", err);
        else console.log("File deleted successfully.");
      });

      console.log("Audio sent successfully.");
    } catch (error) {
      console.error("Error occurred:", error);
      message.reply(`An error occurred: ${error.message}`);
    }
  }
};

// Function to download a track from a URL
async function downloadTrack(url) {
  const stream = await getStreamFromURL(url);
  const filePath = `${__dirname}/tmp/${randomString()}.mp3`;

  // Ensure the tmp directory exists
  await fs.ensureDir(`${__dirname}/tmp`);

  const writeStream = fs.createWriteStream(filePath);
  stream.pipe(writeStream);

  return new Promise((resolve, reject) => {
    writeStream.on('finish', () => resolve(filePath));
    writeStream.on('error', reject);
  });
          }