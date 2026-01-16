// npm packages
const { execSync } = require('child_process');
const path = require('path');
const cron = require('node-cron');
const moment = require('moment-timezone');

// external js
const scrapeBtcEtfData = require('./scrape');
const updateCsv = require('./updateCsv');

// the csv repo path
const REPO_DIR = path.join(__dirname);

// dat higher order function
async function updateBtcEtfFlows() {
    try {
        
        // pull latest repo state
        console.log('Pulling latest repo...');
        execSync(`git -C ${REPO_DIR} pull --allow-unrelated-histories`, { stdio: 'inherit' });

        // scrape the site
        const scrapedData = await scrapeBtcEtfData();
        if (!scrapedData || scrapedData.length === 0) {
            console.error('No scraped data found.');
            return;
        };

        // update the csv with any new values
        await updateCsv(scrapedData);

        // commit and push changes to Github
        console.log('Committing changes...');
        execSync(`git -C ${REPO_DIR} add .`, { stdio: 'inherit' });
        execSync(`git -C ${REPO_DIR} commit -m "Auto-update BTC ETF data"`, { stdio: 'inherit' });
        execSync(`git -C ${REPO_DIR} push`, { stdio: 'inherit' });
        console.log('✔️ All done. Data updated and pushed.');

    } catch (error) {
        console.error('❌ Failed to update BTC ETF flow data:', error);
    };
};
// run dis bitch
updateBtcEtfFlows();

// schedule to run every hour (or more often if needed)
cron.schedule('0 * * * *', async () => {
    const nowPST = moment().tz('America/Los_Angeles'); // PST timezone cause west coast is best coast
  
    // check if we are at the 6th hour (6am)
    if (nowPST.hour() === 6) {
      console.log(`⏰ Running BTC ETF update at ${nowPST.format()}`);
      await updateBtcEtfFlows();
    };
});