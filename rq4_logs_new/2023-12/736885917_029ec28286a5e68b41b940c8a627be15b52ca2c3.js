import React from "react";
import "./App.css";
import Tabs from "react-bootstrap/Tabs";
import Tab from "react-bootstrap/Tab";
import Sidebar from "./Sidebar";
import CourseArea from "./CourseArea";
// import { Button } from "react-bootstrap";
import 'antd/dist/antd.css';
import { Drawer, Button } from 'antd';
import { Cart4 } from 'react-bootstrap-icons';

/**
 * The main application component.
 *
 */
class App extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      allCourses: [], // All the courses fetched from the server.
      filteredCourses: [], // The courses to be displayed in the CourseArea under Search tab.
      subjects: [], // The list of unique subjects fetched from the server.
      completedCourses: [], // The list of completed courses.
      cartCourses: [], // The list of courses in the cart.
      imgLink: [{"iLink":"https://leverageedu.com/blog/wp-content/uploads/2020/04/Media-Psychology.jpg"},
      {"iLink":"https://leverageedu.com/blog/wp-content/uploads/2020/04/Media-Psychology1.jpg"},
      {"iLink":"https://leverageedu.com/blog/wp-content/uploads/2020/04/Media-Psychology2.jpg"},
      {"iLink":"https://leverageedu.com/blog/wp-content/uploads/2020/04/Media-Psychology3.jpg"},
      {"iLink":"https://leverageedu.com/blog/wp-content/uploads/2020/04/Media-Psychology4.jpg"},
      {"iLink":"https://leverageedu.com/blog/wp-content/uploads/2020/04/Media-Psychology5.jpg"},
      {"iLink":"https://leverageedu.com/blog/wp-content/uploads/2020/04/Media-Psychology6.jpg"},
      {"iLink":"https://leverageedu.com/blog/wp-content/uploads/2020/04/Media-Psychology7.jpg"},
      {"iLink":"https://leverageedu.com/blog/wp-content/uploads/2020/04/Media-Psychology8.jpg"},
      {"iLink":"https://leverageedu.com/blog/wp-content/uploads/2020/04/Media-Psychology9.jpg"},
      {"iLink":"https://leverageedu.com/blog/wp-content/uploads/2020/04/Media-Psychology10.jpg"},
      {"iLink":"https://leverageedu.com/blog/wp-content/uploads/2020/04/Media-Psychology11.jpg"},
      {"iLink":"https://leverageedu.com/blog/wp-content/uploads/2020/04/Media-Psychology12.jpg"},
      {"iLink":"https://leverageedu.com/blog/wp-content/uploads/2020/04/Media-Psychology13.jpg"},
      {"iLink":"https://leverageedu.com/blog/wp-content/uploads/2020/04/Media-Psychology14.jpg"},
      {"iLink":"https://leverageedu.com/blog/wp-content/uploads/2020/04/Media-Psychology15.jpg"},
      {"iLink":"https://leverageedu.com/blog/wp-content/uploads/2020/04/Media-Psychology16.jpg"},
      {"iLink":"https://leverageedu.com/blog/wp-content/uploads/2020/04/Media-Psychology17.jpg"},
      {"iLink":"https://leverageedu.com/blog/wp-content/uploads/2020/04/Media-Psychology18.jpg"},
      {"iLink":"https://leverageedu.com/blog/wp-content/uploads/2020/04/Media-Psychology19.jpg"},
    ],
    visible: false
    };
  }


  /**
   * When the component mounts, fetch the classes data from the server.
   * Save the data in the state.
   *
   */
   componentDidMount() {
    // Fetch all the courses from the server
    fetch("https:")
      .then((res) => res.json())
      .then((data) => {
        this.setState({
          allCourses: data,
          filteredCourses: data,
          subjects: this.getSubjects(data),
        });

        return data;
      })
      .then((allCourses) => {
        // fetch all the completed courses
        fetch(
          "https:"
        )
          .then((res) => res.json())
          .then((completedCourseNumbers) => {
            this.setState({
              completedCourses: completedCourseNumbers.data.map(
                (courseNumber) =>
                  allCourses.find((course) => course.number === courseNumber)
              ),
            });
          });
      })
      .catch((err) => console.log(err));
  }

  // Callback function that adds a new course to the cartCourses state
  addCartCourse = (course) => {
    // Duplicate check
    if (
      this.state.cartCourses.some(
        (cartCourse) => cartCourse.number === course.number
      )
    ) {
      console.log(`${course.number} is already in the cart`);
    } else {
      this.setState({
        cartCourses: [...this.state.cartCourses, course],
      });
    }
  };

  // Callback function that removes a course from the cartCourses state
  removeCartCourse(course) {
    this.setState({
      cartCourses: this.state.cartCourses.filter(
        (cartCourse) => cartCourse.number !== course.number
      ),
    });
  }

  getSubjects(data) {
    // Get all the subjects from the JSON of fetched courses.
    // Return a list of unique subjects.

    let subjects = [];
    subjects.push("All");

    for (const course of Object.values(data)) {
      if (subjects.indexOf(course.subject) === -1)
        subjects.push(course.subject);
    }

    return subjects;
  }

  // Callback function that sets the rating of a course
  setRating(courseNumber, rating) {
    this.setState({
      completedCourses: this.state.completedCourses.map((course) => {
        if (course.number === courseNumber) {
          course.rating = rating;
        }
        return course;
      }),
    });
  }

  // Returns the number of courses that are not rated yet.
  getNumCoursesNeedsRating() {
    const numRatedCourses = this.state.completedCourses.filter(
      (course) => course.rating !== undefined
    ).length;

    return this.state.completedCourses.length - numRatedCourses;
  }

  setCourses(courses) {
    // This is a callback function for the filteredCourses state.
    // Set the courses to be displayed in the CourseArea under Search tab.
    // Refer to the Sidebar component (Sidebar.js) to understand when this is used.
    this.setState({ filteredCourses: courses });
  }

  showDrawer(){
    this.setState({visible: true})
  }

  closeDrawer(){
    this.setState({visible: false})
  }

  render() {
    console.log(this.state.cartCourses)

    return (
      <>
        <Tabs
          defaultActiveKey="search"
          style={{
            position: "fixed",
            zIndex: 1,
            width: "100%",
            backgroundColor: "white",
          }}
        >
          {/* Search Tab */}
          <Tab eventKey="search" title="Search" style={{ paddingTop: "5vh" }} onSelect={()=>{this.showDrawer()}}>
            <Sidebar
              setCourses={(courses) => this.setCourses(courses)}
              courses={this.state.allCourses}
              subjects={this.state.subjects}
            />
            <div style={{ marginLeft: "20vw" }}>
              <CourseArea
                data={this.state.filteredCourses}
                allData={this.state.allCourses}
                cartCourses={this.state.cartCourses}
                addCartCourse={this.addCartCourse.bind(this)}
                removeCartCourse={this.removeCartCourse.bind(this)}
                imgs = {this.state.imgLink}
              />
            </div>
            
          </Tab>

          {/* Completed Courses Tab */}
          <Tab
            eventKey="completedCourses"
            title={`Completed Courses (${this.getNumCoursesNeedsRating()} needs rating)`}
            style={{ paddingTop: "5vh" }}
          >
            <div style={{ marginLeft: "5vw" }}>
              <CourseArea
                data={this.state.completedCourses}
                allData={this.state.allCourses}
                setRating={this.setRating.bind(this)}
                mode="completed"
              />
            </div>
          </Tab>

        </Tabs>
        
        <Button type="primary" onClick={()=>{this.showDrawer()}} className='floatButton' size="large" shape="round" icon={<Cart4/>}>
                Cart(1)
        </Button>
        <Drawer title="Cart" placement="right" size="large" onClose={()=>{this.closeDrawer()}} visible={this.state.visible}>
            <div>
              <CourseArea
                data={this.state.filteredCourses}
                allData={this.state.allCourses}
                cartCourses={this.state.cartCourses}
                addCartCourse={this.addCartCourse}
                removeCartCourse={this.removeCartCourse.bind(this)}
                mode="cart"
              />
            </div>
         
        </Drawer>
      </>
    );
  }
}

export default App;