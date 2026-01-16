const { exec } = require('child_process');
const path = require('path');
const fs = require('fs');

const inputVideo = 'input.mp4';

const outputDirectory = 'output_hls';
const outputFile = path.join(outputDirectory, 'output.m3u8');

const ffmpegPath = path.join(__dirname, 'ffmpeg');


// Tạo thư mục lưu hls 
if (!fs.existsSync(outputDirectory)) {
    try {
        fs.mkdirSync(outputDirectory);
    } catch (err) {
        console.error(`Error creating output directory: ${err.message}`);
        process.exit(1);
    }
}


const ffmpegCommand = `"${ffmpegPath}" -i "${inputVideo}" -codec: copy -start_number 0 -hls_time 10 -hls_list_size 0 -f hls "${outputFile}"`;


// Thực thi lệnh FFmpeg
exec(ffmpegCommand, (error, stdout, stderr) => {
    if (error) {
        console.error(`Error executing FFmpeg: ${error.message}`);
        return;
    }

    if (stderr) {
        console.error(`FFmpeg stderr: ${stderr}`);
        return;
    }

    console.log(`FFmpeg stdout: ${stdout}`);
    console.log('Thành công!');
});