import './App.css';
import React, {Component} from "react";
import Form from './component/Container/Form';
import uniqid from "uniqid";
import PersonalView from './component/VIew/PersonalView';
import ExperienceList from './component/VIew/ExperienceList';
import ProjectList from './component/VIew/ProjectList';
import EducationList from './component/VIew/EducationList';


class App extends Component {
  constructor(){
    super();
    this.state = {
      personal: {
        firstname: 'Will',
        lastname: 'Nguyen',
        email: 'khoi.nguyen@ucalgary.ca',
        phone: '123-456-7890',
        summary: 'Hi! My name is Will and I am a M.Eng. software engineering student, graduating in May 2023. Skilled in multiple programming languages, including Java, Python, and C++, and proficient with various development frameworks and tools. Strong communication and teamwork abilities. ',
      },
      exp:{
        id: uniqid(),
        company: 'Subsurface Dynamics',
        position: 'Software Support Engineer',
        start: '01/10/2022',
        end: '',
        description: 'Devise multiple workflows to test different software functionalities in the production and QA/staging environments',
      },
      edu:{
        id: uniqid(),
        school: 'University of Calgary',
        degree: 'Software Engineering',
        start: '01/05/2021',
        end: '01/05/2023',
      },
      proj:{
        id: uniqid(),
        projectname: 'Real Estate Listing Website',
        description: 'Designed a full-stack app for an online marketplace to sell and view real estate properties',
      },
      expList: [],
      eduList: [],
      projList: [],
    }
  }

  handlePersonalDetailsChange = (e) => {
    const {id, value} = e.target;

    this.setState(prevState => ({
      ...prevState,
      personal: {
        ...prevState.personal,
        [id]: value
      }})
      )
  }

  handleArrayChange = (propertyList, index) => {
    return (e) => {
      const {className, value} = e.target;
      this.setState(prevState => ({
        ...prevState,
        [propertyList]: [
          ...prevState[propertyList].slice(0, index),
          {
            ...prevState[propertyList][index],
            [className]: value
          },
          ...prevState[propertyList].slice(index + 1)
        ]
      }), () => {console.log(this.state)})
    }
  }

  onAddEdu = (e) => {
    e.preventDefault();
    this.setState(prevState => ({
      ...prevState,
      eduList: this.state.eduList.concat(this.state.edu),
      edu:{
        id: uniqid(),
        school: '',
        degree: '',
        start: '',
        end: '',
      },
    }))
  };

  onAddProj = (e) => {
    e.preventDefault();
    this.setState(prevState => ({
      ...prevState,
      projList: this.state.projList.concat(this.state.proj),
      proj:{
        id: uniqid(),
        projectname: '',
        description: '',
      },
    }))
  };

  onAddExp = (e) => {
    e.preventDefault();
    this.setState(prevState => ({
      ...prevState,
      expList: this.state.expList.concat(this.state.exp),
      exp:{
        id: uniqid(),
        company: '',
        position: '',
        start: '',
        end: '',
        description: '',
      },
    }))
  };

  onDelete = (propertyList, id) => {
    this.setState(prevState => ({
      ...prevState,
      [propertyList] : [...prevState[propertyList]].filter((item) => item.id !== id)
    }))
  }

  render(){

    return(
      <div>
        <h1>CV Builder</h1>
        <div id='container'>
          <Form 
          {...this.state}
          handlePersonalDetailsChange={this.handlePersonalDetailsChange} 
          handleArrayChange={this.handleArrayChange}
          onAddExp={this.onAddExp}
          onAddProj={this.onAddProj}
          onAddEdu={this.onAddEdu}
          onDelete={this.onDelete}
          />

          <div className='output'>
            <PersonalView {...this.state.personal}/>
            {this.state.expList.length > 0 ? (
              <ExperienceList heading={'Work Experience'} expList={this.state.expList}/>
            ) : (<div></div>)}

            {this.state.projList.length > 0 ? (
              <ProjectList heading={'Projects'} projList={this.state.projList}/>
            ) : (<div></div>)}

            {this.state.eduList.length > 0 ? (
              <EducationList heading={'Education'} eduList={this.state.eduList}/>
            ) : (<div></div>)}
          </div>
        </div>
      </div>
    )
  }
}

export default App;