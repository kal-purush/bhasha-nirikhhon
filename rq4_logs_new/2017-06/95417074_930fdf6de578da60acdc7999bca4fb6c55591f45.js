import React from 'react'
import axios from 'axios'
import ShowTopics from './component/ShowTopics'
class App extends React.Component{
	constructor(){
		super()
		this.state = {
			data: [],
			tabs: ['all', 'share', 'ask', 'job', 'good'],
			tab: 'all'
		}
	}
	componentDidMount(){
		axios.get('https://cnodejs.org/api/v1/topics')
			.then( res => this.setState({data: res.data.data}) )
			.catch( err => alert(err) )
	}
	handleClick(tab){
		// console.log(tab)
		this.setState({tab: tab})
		if (tab==='all') {tab=''}; 
		axios.get('https://cnodejs.org/api/v1/topics?tab='+tab)
			.then( res => this.setState({data: res.data.data}) )
			.catch( err => alert(err) )
	}
	render(){
		console.log(this.state.data)
		let {data, tabs, tab} = this.state;
		let tabsSort = {all: '全部', ask: '问答', share: '分享', job: '工作', good: '精华'}
		return(
			<div>
				{
					tabs.map( (item, i) => <button style={{background: tab===item ? 'green' : '#eee'}} key={i} onClick={this.handleClick.bind(this, item)}>{tabsSort[item]}</button>)
				}
				<ShowTopics data={data} />
			</div>
		)
	}
}

export default App