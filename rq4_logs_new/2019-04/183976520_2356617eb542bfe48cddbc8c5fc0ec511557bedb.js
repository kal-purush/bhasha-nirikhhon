import React from 'react'
import ReactDOM from 'react-dom'
import Web3 from 'web3'
import './../css/index.css'

class App extends React.Component {
   constructor(props){
      super(props)
      this.state = {
         numberOfBets: 0,
         minimumBet: 2,
         totalBet: 0,
         maxAmountOfBets: 5,
         plugInInstalled: true
      }

      if(typeof web3 != 'undefined'){
         console.log("Using web3 detected from external source like Metamask")
         this.web3 = new Web3(web3.currentProvider);
         const MyContract = this.web3.eth.contract([
      	{
      		"constant": false,
      		"inputs": [
      			{
      				"name": "numberToBet",
      				"type": "uint256"
      			}
      		],
      		"name": "bet",
      		"outputs": [],
      		"payable": true,
      		"stateMutability": "payable",
      		"type": "function"
      	},
      	{
      		"constant": false,
      		"inputs": [],
      		"name": "distributePrizes",
      		"outputs": [],
      		"payable": false,
      		"stateMutability": "nonpayable",
      		"type": "function"
      	},
      	{
      		"constant": false,
      		"inputs": [],
      		"name": "generateWinningNumber",
      		"outputs": [
      			{
      				"name": "",
      				"type": "uint256"
      			}
      		],
      		"payable": false,
      		"stateMutability": "nonpayable",
      		"type": "function"
      	},
      	{
      		"constant": false,
      		"inputs": [],
      		"name": "resetData",
      		"outputs": [],
      		"payable": false,
      		"stateMutability": "nonpayable",
      		"type": "function"
      	},
      	{
      		"inputs": [],
      		"payable": false,
      		"stateMutability": "nonpayable",
      		"type": "constructor"
      	}
         ]);
         this.state.ContractInstance = MyContract.at("0xe3be847d5516ebcc7b6bdffe9dd65f61d551dc87");
         window.a = this.state;
      } else {
         console.log("No web3 detected. Falling back to http://localhost:8545. You should remove this fallback when you live, as it's inherently insecure. Consider switching to Metamask for development. More info here: http://truffleframework.com/tutorials/truffle-and-metamask");
         this.web3 = new Web3(new Web3.providers.HttpProvider("http://localhost:8545"))
         this.state.plugInInstalled = false;
      }
   }

   componentDidMount(){
      this.updateState()
      this.setupListeners()
      setInterval(this.updateState.bind(this), 7e3)
   }

   updateState(){
      this.state.ContractInstance.timeStart((err, result) => {
         if(result != null){
            this.setState({
               timeStart: parseInt(result)
            })
         }
      })
      this.state.ContractInstance.numberOfBets((err, result) => {
         if(result != null){
            this.setState({
               numberOfBets: parseInt(result)
            })
         }
      })
      this.state.ContractInstance.maxAmountOfBets((err, result) => {
         if(result != null){
            this.setState({
               maxAmountOfBets: parseInt(result)
            })
         }
      })
      this.state.ContractInstance.minAmountOfBets((err, result) => {
         if(result != null){
            this.setState({
               minAmountOfBets: parseInt(result)
            })
         }
      })
   }

   // Listen for events and executes the voteNumber method
   setupListeners(){
      let liNodes = this.refs.numbers.querySelectorAll('li')
      liNodes.forEach(number => {
         number.addEventListener('click', event => {
            event.target.className = 'number-selected'
            this.voteNumber(parseInt(event.target.innerHTML), done => {

               // Remove the other number selected
               for(let i = 0; i < liNodes.length; i++){
                  liNodes[i].className = ''
               }
            })
         })
         number.addEventListener('mouseover', event => {
            event.target.className = 'number-selected'
         })
         number.addEventListener('mouseout', event => {
            event.target.className = ''
         })
      })
   }

   voteNumber(number, cb){
      let bet = this.refs['ether-bet'].value

      if(!bet) bet = 0.1

      if(parseFloat(bet) < this.state.minimumBet){
         alert('You must bet more than the minimum')
         cb()
      } else {
         this.state.ContractInstance.bet(number, {
            gas: 300000,
            from: web3.eth.accounts[0],
            value: web3.toWei(bet, 'ether')
         }, (err, result) => {
            cb()
         })
      }
   }

   render(){
      if (this.state.plugInInstalled) {
         return (
            <div className="main-container">
               <h1>Bet for your best number and win huge amounts of Ether</h1>

                  <div className="block">
                     <b>Number of bets:</b> &nbsp;
                     <span>{this.state.numberOfBets}</span>
                  </div>

                  <div className="block">
                     <b>Total ether bet:</b> &nbsp;
                     <span>{this.state.timeStart} ether</span>
                  </div>

                  <div className="block">
                     <b>Minimum bet:</b> &nbsp;
                     <span>{this.state.minAmountOfBet} ether</span>
                  </div>

                  <div className="block">
                     <b>Max amount of bets:</b> &nbsp;
                     <span>{this.state.maxAmountOfBets}</span>
                  </div>

               <hr></hr>

               <h2>Vote for the next winning number yeeet</h2>

               <label>
                  <b>How much Ether do you want to bet? <input className="bet-input" ref="ether-bet" type="number" placeholder={this.state.minimumBet}/></b> ether
                  <br/>
               </label>

               <ul ref="numbers">
                  <li>1</li>
                  <li>2</li>
                  <li>3</li>
                  <li>4</li>
                  <li>5</li>
                  <li>6</li>
                  <li>7</li>
                  <li>8</li>
                  <li>9</li>
                  <li>10</li>
               </ul>

               <hr></hr>

               <div><i>Only working with the Ropsten Test Network</i></div>
               <div><i>You can only vote once per account</i></div>
               <div><i>Your vote will be reflected when the next block is mined</i></div>
            </div>
         )
      } else {
         return (
            <div className="main-container">
               <h1> Hi! If you would like to see the contents of this page, please install MetaMask (metamask.io)
                     and select the Ropsten Test Network!
               </h1>
            </div>
         )
      }
   }
}

ReactDOM.render(
   <App />,
   document.querySelector('#root')
)