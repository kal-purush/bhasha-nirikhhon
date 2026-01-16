import React from 'react';
import { Redirect } from 'react-router-dom';

class Search extends React.Component{
    constructor(props, context){
        super(props, context);
        this.state = {
            apiURL: 'http://140.119.163.194:3004/',
            userName: ''
        };
        this.handleChange = this.handleChange.bind(this);
        this.handleSubmit = this.handleSubmit.bind(this);
    }

    // 取得輸入值
    handleChange(event) {
        switch (event.target.placeholder){
            case '搜尋使用者':{
                this.setState({userName: event.target.value});
                break;
            }
            default: {
                break;
            }
        }
    }
    // 提交表單
    handleSubmit(event) {
        event.preventDefault();
    }

    componentDidMount(){
        fetch(this.state.apiURL + 'search_user', {
            method: 'get',
            headers: {
                'Accept': 'application/json, text/plain, */*',
                'Content-Type': 'application/json'
            },
        }).then(res => res.json())
            .then(res => {
                console.log(res);
                console.log(res[0]._id);
                console.log(res[0].userName);
                console.log(res[0].avatarLink[res[0].avatarLink.length-1]);
            });
    }

    render(){

        return (
            <div>
                <br/><br/><br/>
                <form onSubmit={this.handleSubmit}>
                    <div className="inputFieldAlign">
                        <input className="inputField" type="text" placeholder="搜尋使用者" onChange={this.handleChange} />
                    </div>
                    <input className="inputField inputField_loginOrSignUp" type="submit" value="登入" />
                </form>
            </div>
        );
    }
}

export default Search;