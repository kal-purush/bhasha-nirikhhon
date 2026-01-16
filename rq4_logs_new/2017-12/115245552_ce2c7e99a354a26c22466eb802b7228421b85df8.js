import React, { Component } from 'react'
import { Layout, Card, Table } from 'antd'

const { Content, Header } = Layout


const columns = [
    {
        title: 'ลำดับที่',
        dataIndex: 'number',
        // render: text => <a href="#">{text}</a>,
      },{
    title: 'โรคและอาการ',
    dataIndex: 'name',
    // render: text => <a href="#">{text}</a>,
  }, {
    title: 'เพศหญิง',
    className: 'column-money',
    dataIndex: 'girl',
  }, {
    title: 'เพศชาย',
    className: 'column-money',
    dataIndex: 'man',
  },{
    title: 'ผู้ป่วยนอกจำนวน',
    className: 'column-money',
    dataIndex: 'memberOut',
  }, {
    title: 'ผู้ป่วยในจำนวน',
    className: 'column-money',
    dataIndex: 'memberIn',
  },
  {
    title: 'รวม',
    className: 'column-money',
    dataIndex: 'totle',
  }
]

  const data = [

    {
        key: '1',
        number: '1',
        name: 'ไข้หวัด',
        girl: '20',
        man: '1',
        memberOut: '0',
        memberIn: '21',
        totle: '21',
    },
    {
        key: '2',
        number: '2',
        name: 'ท้องเสีย',
        girl: '3',
        man: '8',
        memberOut: '6',
        memberIn: '5',
        totle: '11',
    },
    {
        key: '3',
        number: '3',
        name: 'ปวดหัว',
        girl: '14',
        man: '3',
        memberOut: '0',
        memberIn: '17',
        totle: '17',
    },
  ];  

class ReportByDisease extends Component {
    onSubmit = () => {
        this.props.history.push('/auth/auth')
    }
    render() {
        return (
            <Layout>
                <Header />
                <Content style={{ margin: '0 16px', marginTop: 15 }}>
                    <Card title="รายงานผู้ป่วยประจำเดือน" bordered={false} >
                        <Table
                            columns={columns}
                            dataSource={data}
                            bordered
                            title={() => 'รายงานผู้ป่วยจำแนกตามโรค'}
                        />
                    </Card>
                </Content>
            </Layout>
        )
    }
}

export default ReportByDisease