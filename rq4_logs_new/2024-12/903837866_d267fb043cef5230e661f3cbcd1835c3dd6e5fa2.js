const fs = require('fs').promises;

async function compressJSON(inputFile, outputFile) {
    try {
        // Đọc file input
        const data = await fs.readFile(inputFile, 'utf8');
        const originalSize = Buffer.byteLength(data, 'utf8');

        // Parse JSON
        const jsonData = JSON.parse(data);
        const [query] = Object.keys(jsonData);
        const events = jsonData[query];

        // Quy tắc thay thế tên trường
        const rules = {
            'id': 'i',
            'season_id': 's',
            'stage_id': 'g',
            'group_num': 'n',
            'round_num': 'r',
            'start_time': 't',
            'start_timestamp': 'p',
            'sport_event_status': 'e',
            'status_id': 'd',
            'updated_at': 'u',
            'record_updated_at': 'c',
            'home_team_id': 'h',
            'away_team_id': 'a',
            'competition_id': 'o',
            'lineup': 'l',
            'venue_id': 'v',
            'referee_id': 'f',
            'related_id': 'x',
            'agg_score': 'y',
            'corners': 'cr',
            'red_card': 'rd',
            'yellow_card': 'yd',
            'penalty_score': 'ps',
            'regular_score': 'rs',
            'overTime_score': 'os',
            'half_time_score': 'hs',
            'away_score': 'as',
            'home_score': 'hs'
        };

        // Nén dữ liệu
        const compressedEvents = events.map(event => {
            const compressed = {};

            for (const [key, value] of Object.entries(event)) {
                const newKey = rules[key] || key;

                if (key === 'sport_event_status' && value) {
                    try {
                        const statusObj = JSON.parse(value);
                        const newStatus = {
                            d: statusObj.status_id,
                            a: {},
                            h: {}
                        };

                        // Nén away_score và home_score
                        ['away_score', 'home_score'].forEach(scoreType => {
                            const scores = statusObj[scoreType];
                            const target = scoreType === 'away_score' ? newStatus.a : newStatus.h;

                            for (const [scoreKey, scoreValue] of Object.entries(scores)) {
                                const newScoreKey = rules[scoreKey] || scoreKey;
                                if (scoreValue !== null && scoreValue !== '') {
                                    target[newScoreKey] = scoreValue;
                                }
                            }
                        });

                        compressed[newKey] = newStatus;
                    } catch (e) {
                        compressed[newKey] = value;
                    }
                } else if (value !== null && value !== '') {
                    compressed[newKey] = value;
                }
            }

            return compressed;
        });

        // Tạo object nén cuối cùng
        const finalCompressed = {
            q: query,
            d: compressedEvents
        };

        // Chuyển thành chuỗi JSON và ghi file
        const compressedJSON = JSON.stringify(finalCompressed);
        await fs.writeFile(outputFile, compressedJSON);

        // Tính toán và hiển thị thống kê
        const compressedSize = Buffer.byteLength(compressedJSON, 'utf8');
        const savedBytes = originalSize - compressedSize;
        const savedPercentage = ((savedBytes / originalSize) * 100).toFixed(2);

        console.log(`Kích thước gốc: ${formatSize(originalSize)}`);
        console.log(`Kích thước sau nén: ${formatSize(compressedSize)}`);
        console.log(`Giảm được: ${formatSize(savedBytes)} (${savedPercentage}%)`);
    } catch (error) {
        console.error('Lỗi:', error);
    }
}

// Hàm format kích thước file
function formatSize(bytes) {
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    if (bytes === 0) return '0 Byte';
    const i = parseInt(Math.floor(Math.log(bytes) / Math.log(1024)));
    return Math.round(bytes / Math.pow(1024, i), 2) + ' ' + sizes[i];
}

// Sử dụng
compressJSON('data.json', 'compressed.json')
    .then(() => console.log('Nén thành công!'))
    .catch(console.error);