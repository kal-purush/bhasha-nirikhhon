import {Component} from 'react'
import Cookies from 'js-cookie'
import {TailSpin} from 'react-loader-spinner'
import './index.css'
import {BarChart,Bar,XAxis,YAxis,Tooltip,Legend} from 'recharts'
import {BsCurrencyDollar} from 'react-icons/bs'

const CustomBar = (props) => {
    const { x, y, width, height, fill } = props; //styling of bar in barchart ... copied from chatgpt
    const radius = 6; 
    return (
        <g>
          <path
            d={`M${x},${y + height} 
              L${x},${y + radius} 
              Q${x},${y} ${x + radius},${y} 
              L${x + width- radius},${y} 
              Q${x + width},${y} ${x + width},${y + radius} 
              L${x + width},${y + height} 
              Z`}
            fill={fill}
          />
        </g>
      );
}

class TransactionChart extends Component {
    
    state = {data:[],isLaoding:true,total_credit:0,total_debit:0}

    componentDidMount() {
        this.getSomeTransactions()
    }

    modifyData = (dummyData) => {
        let dates = [];
        dummyData = dummyData.map(item => {
            let parts = item.date.slice(0,10).split('-')
            
            dates.push(parts[1]+'/'+parts[2]+'/'+parts[0]);
            return({...item,date:parts[1]+'/'+parts[2]+'/'+parts[0]})
        })
        dates = [...new Set(dates)];
        let newData = dates.map(date =>{
            return({date,DEBIT:0,CREDIT:0})
        })
        let total_credit = 0
        let total_debit = 0
        for(let i =0;i<dummyData.length;i++){
            for(let j=0;j<newData.length;j++){
                if (dummyData[i].date === newData[j].date){
                    if(dummyData[i].type === 'credit'){
                        newData[j].CREDIT  += dummyData[i].sum
                        total_credit += dummyData[i].sum
                    }
                    else{
                        newData[j].DEBIT  += dummyData[i].sum
                        total_debit += dummyData[i].sum
                    }
                }
            }
        }
    return {newData,total_credit,total_debit}
    }

    getSomeTransactions = async() => {
        
        const token = Cookies.get('token');

        const url = token === '3' ? 'https://bursting-gelding-24.hasura.app/api/rest/daywise-totals-last-7-days-admin':'https://bursting-gelding-24.hasura.app/api/rest/daywise-totals-7-days'
        const options = token === '3' ? {
            method:'GET',
            headers: {
            
                    'content-type':'application/json',
                    'x-hasura-admin-secret':'g08A3qQy00y8yFDq3y6N1ZQnhOPOa4msdie5EtKS1hFStar01JzPKrtKEzYY2BtF',
                    'x-hasura-role':'admin',
                    
                

            }
        }:{
            method:'GET',
            headers: {
            
                    'content-type':'application/json',
                    'x-hasura-admin-secret':'g08A3qQy00y8yFDq3y6N1ZQnhOPOa4msdie5EtKS1hFStar01JzPKrtKEzYY2BtF',
                    'x-hasura-role':'user',
                    'x-hasura-user-id':token
                

            }
        }
        const response = await fetch(url,options)
        const data = await response.json();
        //console.log("data",data['last_7_days_transactions_credit_debit_totals']);
        let dummyData;
        if (token === '3') dummyData = data['last_7_days_transactions_totals_admin']
    
        else dummyData = data['last_7_days_transactions_credit_debit_totals']
        console.log(dummyData);
        let {newData,total_credit,total_debit }= this.modifyData(dummyData)
        
        if(response.ok === true && newData.length > 0){
            this.setState({data:newData,isLoading:false,total_credit,total_debit})
        }
    }

    renderLoading = () => {
        return(<div style ={{margin:'auto'}}>
                 <TailSpin type="TailSpin" color="#0284c7" height={50} width={50} />
                    </div>)
    }

    renderData = () => {
        const {data} = this.state 

        return(<div>
        
            <BarChart width={1000}
          height={300}
          data={data}
          margin={{
            top: 30,
            right: 30,
            left: 20,
            bottom: 30,
          }}>
            
            
          <XAxis dataKey= 'date' />
          <YAxis />
          <Legend verticalAlign = "top" align = "right"/>
          <Tooltip  />
          
          <Bar dataKey= 'DEBIT' fill='#fcaa0b'  shape = {<CustomBar/>}/>
          <Bar dataKey= 'CREDIT' fill = '#2d06ff' shape = {<CustomBar/>}/>
          </BarChart>
        </div>)
    }

    render(){
        const {isLoading,total_credit,total_debit} = this.state
        console.log(isLoading,total_credit,total_debit);
        return(<div className='chart-page'>
        <h1 className='latest-transaction-heading'>Debit & Credit Overview</h1>
        <div className='chart-container'>
                {isLoading ? '':<p className='description'><span className='amt'><BsCurrencyDollar className='dollar'/>{total_credit}</span> credited this month & <span className='amt'><BsCurrencyDollar className='dollar'/>{total_debit}</span> debited over past few days.. </p>}
                {isLoading ? this.renderLoading() : this.renderData() }
        </div>
        </div>)
    }
}

export default TransactionChart