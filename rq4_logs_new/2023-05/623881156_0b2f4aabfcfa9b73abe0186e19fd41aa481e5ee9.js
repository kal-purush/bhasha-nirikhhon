import React from "react";
import { useContext, useState } from "react";
export const Erep = () => {
  const courses = [
    {
      courseName: "MERN course",
      courseDesc: "Frontend + Backend",
      courseImage:
        "https://hopetutors.com/wp-content/uploads/2017/08/mern-stack-training.png",
      courseLink: "https://youtu.be/ORyi6tTMNqE",
    },
    {
      courseName: "DEVOPS course",
      courseDesc: "Docker,Kubernetes etc.",
      courseImage:
        "https://cdn.dribbble.com/users/13574/screenshots/9711275/logo-devops.png",
      courseLink:
        "https://www.youtube.com/watch?v=ZbG0c87wcM8&list=PL9gnSGHSqcnoqBXdMwUTRod4Gi3eac2Ak",
    },
    {
      courseName: "DJANGO course",
      courseDesc: "Fullstack development with python",
      courseImage: "https://cdn.mindmajix.com/courses/django-training.png",
      courseLink:
        "https://www.youtube.com/watch?v=C1NgOmoOszc&list=PLjVLYmrlmjGcyt3m6rt21nfjhYSWP_Ue_",
    },
  ];
  return (
    <div>
      {courses.map((item, index) => (
        <Course
          courseName={item.courseName}
          courseImage={item.courseImage}
          courseDesc={item.courseDesc}
          courseLink={item.courseLink}
          key={index}
        />
      ))}
    </div>
  );
};
function Course({ courseName, courseImage, courseDesc, courseLink }) {
  console.log(courseImage);
  return (
    <div className="container1">
      <div className="product_container1">
        <div className="productImage1">
          <img className="img1" src={courseImage}></img>
        </div>
        <div className="productName1">{courseName}</div>
        <div className="productDesc1">{courseDesc}</div>
        <a className="view" href={courseLink} target="_blank">
          VIEW
        </a>
      </div>
    </div>
  );
}