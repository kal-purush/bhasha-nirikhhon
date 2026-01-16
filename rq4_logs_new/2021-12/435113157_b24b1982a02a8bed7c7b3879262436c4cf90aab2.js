
// import Arweave from 'arweave';
// import { useState } from "react"

import {useEffect, useState} from "react";
import TestWeave from "testweave-sdk";


function SaveText (props) {

    let [ar_link, setArLink] = useState('None')
    let [saveHtml, setSaveHtml] = useState('')
    let [current_address, setCurrentAddress] = useState('None')
    let [current_balance, setCurrentBalance] = useState('0')
    let [trans_result, setTransResult] = useState('None')

    useEffect(()=> {
        if (props.arweave) {
            let arweave = props.arweave

            TestWeave.init(arweave).then(async (testArweave) => {
                console.log("Test-Weave running...")
                console.log(testArweave.rootJWK)

                // Old code is props.key_json
                arweave.wallets.jwkToAddress(testArweave.rootJWK).then((addr) => {
                    console.log('jwk to address:', addr);
                    setCurrentAddress(addr)
                    arweave.wallets.getBalance(addr).then((bal) => {
                        const balance = arweave.ar.winstonToAr(bal)
                        console.log('get balance: ', balance)
                        setCurrentBalance(balance)
                    })
                })
            })

        }
    }, [props.arweave])


    const saveContent = async function () {
        let html_txt = document.getElementById('saveHtml').value
        console.log("Save content.", html_txt)

        if (props.arweave) {
            let arweave = props.arweave

            TestWeave.init(arweave).then(async (testArweave) => {
                console.log("Test-Weave running...")
                console.log(testArweave.rootJWK)

                let result = await arweave.createTransaction({
                    // data: Buffer.from('Some data', 'utf8')
                    data: html_txt
                }, testArweave.rootJWK)
                //
                await arweave.transactions.sign(result, testArweave.rootJWK);
                console.log(result)

                var response = await arweave.transactions.post(result);

                console.log(response)
                // if (response.status == 200) {
                //     document.write("Puzzle submitted successfully. It should appear within 2-4 minutes on the <a href='index.html'>puzzles list</a>")
                // } else {
                //     document.write(`Internal error (error code ${response.status}) <a href='index.html'>Click here to go back to the puzzles list</a>`)
                // }
            })







        }
    }

    return (
        <div>
            <h2>**  SaveText ** </h2>
            <div>{ar_link}</div>
            <div>
                <div>Current Address : {current_address}</div>
                <div>Current Address : {current_balance}</div>
                <form action='#'>
                    <div>
                        <textarea id='saveHtml'></textarea>
                    </div>
                    <div>
                        <button onClick={saveContent}>
                            Submit.
                        </button>
                    </div>
                </form>
                <div>{trans_result}</div>
            </div>
        </div>
    );
}

export default SaveText;