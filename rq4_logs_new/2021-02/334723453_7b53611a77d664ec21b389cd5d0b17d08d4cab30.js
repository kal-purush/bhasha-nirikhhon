const assert = require('assert')

const Parser = require('./parser')
const util = require('./util')
const fs = require('fs').promises;

const inputPath = "./test_data/maritaldataLarge.csv";
const outputPath = "./tests/ignored_tests/maritaldataLargeNewComSplit.json";
const csvOut = "./tests/ignored_tests/maritaldataLargeNewComSplit.csv";

async function example(){
    const content = await fs.readFile(inputPath,'utf8');
    let arr = util.csvArray(content,/","/);
    arr = util.transpose(arr);
    arr = util.chop(arr,/Margin of Error!!/,-1);
    arr = util.chopColumn(arr,0);
    arr = util.clear(arr,/Estimate!!/);
    arr = util.chop(arr, 0);
    arr = util.clear(arr);
    let str = util.convertArrToCSV(arr);

    await fs.writeFile(csvOut,str).catch((e)=> {console.log('error writing csv file'); console.log(e);})
    let parser = new Parser(arr);
    parser.setHeaders(0,0);
    parser.setProps('row');
    parser.propsToArray(/!!/);
    let json = parser.mapProps();
    json = JSON.stringify(json);
    await fs.writeFile(outputPath,json).catch((e)=> {console.log('error writing file'); console.log(e);})

}
example().catch( (ex) => {
    console.log(`error!\n${ex}`)
})