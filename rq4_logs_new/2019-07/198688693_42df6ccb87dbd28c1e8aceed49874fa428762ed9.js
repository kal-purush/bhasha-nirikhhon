const fetch = require('node-fetch');

const regexp = /var data = (.*);/;

function sleep(ms) 
{
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function fetchNgram(phrases) 
{
    const params = new URLSearchParams();
    params.set('content', phrases.join(','));
    params.set('year_start', '2007');
    params.set('year_end', '2008');
    params.set('corpus', '15');  // English
    params.set('smoothing', '0');

    var errorCounter = 0;
    var json = null;
    var waitingTime = 30000;
    json = await getTheData(errorCounter, waitingTime, json, params);
    if (json === null)
    {
        console.log("json is null after getTheData");
    }

    return JSON.parse(json); 
}



async function getTheData(numberOfErrors, waitingTime, json, params) 
{
    var response;
    try 
    {
        response = await fetch('https://books.google.com/ngrams/graph?' + params.toString());
    } 
    catch (err) 
    {
        console.log("Error fetching. Waiting %s. %s error.", waitingTime/1000, numberOfErrors);
        console.log(err);
        numberOfErrors++;
        await sleep(waitingTime);
        json = await getTheData(numberOfErrors, waitingTime * 2, json, params);
    }
    if (json != null)
    {
        return json;
    }
    else
    {
        const responseText = await response.text();
        const match = regexp.exec(responseText);
        try
        {
            json = match[1];
        }
        catch (err)
        {
            console.log("Error parsing json - no data. Waiting %s. %s error.", waitingTime/1000, numberOfErrors);
            console.log(response);
            numberOfErrors++;
            await sleep(waitingTime);
            json = await getTheData(numberOfErrors, waitingTime * 2, json, params);
        }
        
        return json;
    }
}

module.exports = { fetchNgram, sleep };