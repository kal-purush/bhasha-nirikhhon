import React, {Component} from 'react';
import Form from './Form';
import ReactMarkdown from 'react-markdown';

const axios = require('axios');


export default class DeleteCourse extends Component{
  state= {
    errors:[],
    user:this.props.context.authenticatedUser || null,
    id:this.props.match.params.id,
    course:{},
    courseUser:{}
  }

  async componentDidMount(){
    await this.getCourse(this.state.id)
    
    if (this.state.user === null){
      this.props.history.push('/signin')
    } else if( this.state.user.userId !== this.state.courseUser.id) {
      this.props.history.push('/forbidden')
    }
  }

  getCourse = async function(id) {
    await axios.get(`http://localhost:5000/api/courses/${id}`)
    .then(data=>{
        this.setState({
          course:data.data,
          courseUser:data.data.User
        })
      })
    .catch(err=>{
      console.error(err)
      this.history.props.push('/notfound')})
  }

  render(){
    const {
      errors,
      course,
      courseUser
    }=this.state

  return (
    
    <div>
     <div className="wrap">
        <h1>Delete Course: "{course.title}"</h1>
          <Form 
            cancel={this.cancel}
            errors={errors}
            submit={this.submit}
            submitButtonText="Delete Course"
            elements={() => (
              <div className="wrap">
                <form>
                    <div className="main--flex">
                        <div>
                            <h3 className="course--detail--title">Course Detail</h3>
                            <h4 className="course--name">{course.title}</h4>
                            <p>{`By ${courseUser.firstName} ${courseUser.lastName}`}</p>

                            <ReactMarkdown>{course.description}</ReactMarkdown>
                        </div>
                        <div>
                            <h3 className="course--detail--title">Estimated Time</h3>
                            <p>{course.estimatedTime || 'Not Provided'}</p>

                            <h3 className="course--detail--title">Materials Needed</h3>
                            {course.materialsNeeded? (
                              <ReactMarkdown className="course--detail--list" children={course.materialsNeeded} />)
                              :
                              (<p>None Required</p>)
                            }
                             
                        </div>
                    </div>
                </form>
            </div>
            )} />
      </div>   
    </div>
  );
  }

  change = (event) => {
    const name = event.target.name;
    const value = event.target.value;

    this.setState(() => {
      return {
        [name]: value
      };
    });
  }

  cancel = () => {
    this.props.history.push(`/courses/${this.state.id}`);
   }

   //can't get context (authorizedUser) to register, so fetching cookies each time...
   //Calls Data.js updateCourse to create a PUT request to the API and update the current course
   //Only if the authorized user is the owner to the course
  submit = () => {
    const { context } = this.props;
    const { title, description, estimatedTime, materialsNeeded, userId, user , id} = this.state;
    const course ={title, description, estimatedTime, materialsNeeded, userId} 

    context.data.deleteCourse(id, course, user.emailAddress, user.password)
      .then(errors => {
        if(errors.length){
          this.setState({errors:errors})
        } 
        else{
          console.log('course successfully deleted')
          this.props.history.push(`/`)
        }}
      )
      .catch(err=>{
        console.error(err)
        this.props.history.push('/error')})

  }
}
