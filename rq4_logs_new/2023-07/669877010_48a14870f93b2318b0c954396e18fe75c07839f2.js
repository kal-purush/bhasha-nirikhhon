const axios = require("axios");

funcc()

async function funcc() {

    // let baseUrl = "http://127.0.0.1:8080/"
	let baseUrl = "https://marcuswentz.github.io/chainlink_test_json_url_types/";
    let responseRawJSON = await axios.get(baseUrl);
    let responseDataJSON = responseRawJSON.data;
    console.log(responseRawJSON)
    console.log(responseDataJSON)
    console.log(responseDataJSON.uint256)
    console.log(responseDataJSON.int256)
    console.log(responseDataJSON.bool)
    console.log(responseDataJSON.string)
    console.log(responseDataJSON.bytes32)
    console.log(responseDataJSON.bytes)
}