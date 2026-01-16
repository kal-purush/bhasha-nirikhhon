import React, { Component } from 'react';

export default class LessonInfo extends Component {

    state={
        words:[],
        word: ""
    }

    componentDidMount(){
        this.getWords();
    }

    componentDidUpdate(){
        this.printWords();
    }


    handleChange = (event) => {
        this.setState({
            [event.target.name]:event.target.value
        })
    }

    getWords = () => {
        if(this.props.lesson){
            fetch('http://localhost:3001/words')
            .then(resp => resp.json())
            .then(data => {
                let wordsArray = data.filter(word => word.lesson_id === this.props.lesson.id);
                this.setState({
                    words: wordsArray
                })
            })
        }
        
    }

    printWords = () => {
        let div = document.getElementsByClassName('lesson-info')[0];
        // console.log(this.props.id, this.state.words);
        div.innerHTML = "";

        if(this.state.words.length>0){
            // console.log(this.props.id, this.state.words);
            this.state.words.forEach(word=>{
                div.innerHTML += `<span class="badge badge-warning large">${word.text}</span>`;
            })
        }
    }

    addWord = (event) => {
        event.preventDefault();
        fetch('http://localhost:3001/words',{
            method: "POST",
            headers:{
                "Content-Type": "application/json",
                "Accept": "application/json"
            },
            body: JSON.stringify({
                text: this.state.word,
                lesson_id: this.props.lesson.id
            })
        }).then(resp => resp.json())
        .then(data => {
            console.log(data);
            this.setState({
                words: [...this.state.words, data],
                word: ""
            })
        })
    }
    
    render() {

        return(
            <div className="card lesson-card">
                <div className="card-body">
                    <div>
                        <h5>Class Name:{this.props.lesson.name}</h5>
                        <div>Words:<div className="lesson-info"></div>
                        </div>
                        <p>Secred Code: {this.props.lesson.id}</p>
                    </div>
                    <form className="lessonForm" onSubmit={this.addWord}>
                        <label>add word to class</label>
                        <input type="text" name="word" placeholder="please type in word you want to add" onChange={this.handleChange} value={this.state.word}/>
                        <button type="submit" value="submit" className="btn btn-primary" >+</button>
                    </form>
                </div>
            </div>
        )
    }
}





