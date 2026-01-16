import {Component, ElementRef, OnInit, ViewChild} from '@angular/core';
import {Trajectory} from '../../entity/Trajectory';
import {Course} from '../../entity/course';

class Position{
  x: number;
  y: number;

  constructor(x: number, y: number) {
    this.x = x;
    this.y = y;
  }
}

class VisualNode{
  position:Position;

  constructor(position: Position) {
    this.position = position;
  }
}

@Component({
  selector: 'app-graph-view',
  templateUrl: './graph-view.component.html',
  styleUrls: ['./graph-view.component.css']
})
export class GraphViewComponent implements OnInit {

  nodes: Map<number, VisualNode>  = new Map();
  courses: Map<number, Course> = new Map();
  connections: Map<number, number[]> = new Map();
  semesterLine: Map<number, number> = new Map();

  trajectories: Trajectory[];
  selectedTrajectory: Trajectory;

  @ViewChild('myCanvas')
  myCanvas: ElementRef;
  public context: CanvasRenderingContext2D;

  NODE_WIDTH = 200;
  NODE_HEIGHT = 30;
  HORIZONTAL_GAP = 30;
  VERTICAL_GAP = 45;

  constructor() {
  }

  ngOnInit() {

  }

  ngAfterViewInit(): void {
    this.context = (<HTMLCanvasElement>this.myCanvas.nativeElement).getContext('2d');
  }

  clicked(event) {
  }

  prepareGraph() {
    this.nodes = new Map();
    this.courses = new Map();
    this.connections = new Map();
    this.semesterLine = new Map();

    this.trajectories.forEach(trajectory => {
      let courses = trajectory.courses;
      for(let i = 0; i< courses.length - 1; ++i){
        let from = courses[i].id;
        let to = courses[i + 1].id;

        if(!this.connections.has(from)){
          this.connections.set(from, []);
        }

        this.connections.get(from).push(to);
      }
    });

    this.trajectories.forEach(trajectory => trajectory.courses.forEach(course => this.courses.set(course.id, course)));

    let coursesBySemesters = new Map<number, Course[]>();
    let semesters: number[] = [];
    this.courses.forEach(course => {
      let semester = course.semester;
      if (!coursesBySemesters.has(semester)) {
        coursesBySemesters.set(semester, []);
      }

      coursesBySemesters.get(semester).push(course);

      if (!semesters.includes(semester)) {
        semesters.push(semester);
      }
    });

    let position = new Position(0, 0);

    semesters.sort((a, b)=> a-b);

    semesters.forEach((semester)=> {
      let courses = coursesBySemesters.get(semester);
      let baseCourses: Course[] = [];
      let variableCourses = new Map<number, Course[]>();

      for (let course of courses) {
        if (course.implementsTemplate == true) {
          let templateId = course.templateCourse;
          if (!variableCourses.has(templateId)) {
            variableCourses.set(templateId, []);
          }
          variableCourses.get(templateId).push(course);
        }
        else {
          baseCourses.push(course);
        }
      }

      for (let course of baseCourses) {
        this.nodes.set(course.id, new VisualNode(new Position(0, position.y)));
        position.y += this.NODE_HEIGHT + this.VERTICAL_GAP;
      }

      variableCourses.forEach((value, key) => {
          position.x = 0;
          for (let course of variableCourses.get(key)) {
            this.nodes.set(course.id, new VisualNode(new Position(position.x, position.y)));
            position.x += this.NODE_WIDTH + this.HORIZONTAL_GAP;
          }
          position.y += this.NODE_HEIGHT + this.VERTICAL_GAP;
        });
    });

    // Align center
    let nodesOnSameHeight = new Map<number, VisualNode[]>();
    this.nodes.forEach(value => {
      if(!nodesOnSameHeight.has(value.position.y)){
        nodesOnSameHeight.set(value.position.y, []);
      }

      nodesOnSameHeight.get(value.position.y).push(value);
    });

    let maxCountInLine: Number = 0;
    nodesOnSameHeight.forEach(value => {
      let countInLine = value.length;
      if(countInLine > maxCountInLine){
        maxCountInLine = countInLine;
      }
    });

    let maxWidth = <number>(maxCountInLine) * this.NODE_WIDTH + (<number>(maxCountInLine) - 1) * this.HORIZONTAL_GAP;
    nodesOnSameHeight.forEach(values => {
      let countInLine = values.length;
      let width = countInLine * this.NODE_WIDTH + (countInLine - 1) * this.HORIZONTAL_GAP;
      let delta = (maxWidth - width) / 2;
      values.forEach(node=>{
        node.position.x += delta;
      });
    });

    // Semester delimeters
    this.courses.forEach((course, key) => {
      let semester = course.semester;
      let height = this.nodes.get(key).position.y + this.NODE_HEIGHT + this.VERTICAL_GAP / 2;

      if(!this.semesterLine.has(semester) || this.semesterLine.get(semester) < height){
        this.semesterLine.set(semester, height);
      }
    });
  }

  repaint() {
    this.prepareGraph();

    this.context.fillStyle = 'white';
    this.context.fillRect(0, 0, 1000, 1000);

    if (this.trajectories == null) {
      return;
    }


    this.context.strokeStyle = "#999999";
    this.context.fillStyle = "#999999";
    this.context.lineWidth = 1;
    this.semesterLine.forEach((value, key)=>{
      this.context.beginPath();
      this.context.moveTo(0, value);
      this.context.lineTo(1000, value);
      this.context.stroke();

      this.context.fillText("Semester: " + key, 3, value - 3);
    });

    this.connections.forEach(((value, key) => {

      let fromPos = this.getCenterOfNode(key);

      value.forEach(toNode =>{
        let toPos = this.getCenterOfNode(toNode);

        if(this.selectedTrajectory != null && this.isConnectionSelected(key, toNode)){
          this.context.strokeStyle = "#F28C8C";
          this.context.lineWidth = 3;
        }
        else{
          this.context.strokeStyle = "#1C3B47";
          this.context.lineWidth = 1;
        }

        this.context.beginPath();
        this.context.moveTo(fromPos.x, fromPos.y);
        this.context.lineTo(toPos.x, toPos.y);
        this.context.stroke();
      });
    }));

    this.context.font = "12pt Arial";

    this.nodes.forEach(((value, key) => {

      let nodeColor: string;
      let fontColor: string = "#000000";

      if (this.selectedTrajectory != null && this.isCourseSelected(key)) {
        nodeColor = '#F2D1B3';
      } else {
        nodeColor = '#2D5F73';
      }

      this.context.fillStyle = nodeColor;
      this.context.fillRect(value.position.x, value.position.y, this.NODE_WIDTH, this.NODE_HEIGHT);
      this.context.fillStyle = fontColor;
      this.context.fillText(this.courses.get(key).name, value.position.x, value.position.y + this.NODE_HEIGHT / 2);
    }))
  }

  getCenterOfNode(nodeId: number): Position{
    let nodePosition = this.nodes.get(nodeId).position;
    return new Position(nodePosition.x + this.NODE_WIDTH / 2, nodePosition.y + this.NODE_HEIGHT / 2);
  }

  isCourseSelected(courseId: number): boolean{
    return this.selectedTrajectory.courses.map(course => course.id).some(id => id === courseId);
  }

  setTrajectories(trajectories: Trajectory[]) {
    this.trajectories = trajectories;
    this.selectedTrajectory = null;
    console.log('All trajectories: ' + this.trajectories);

    this.repaint();
  }

  setSelectedTrajectory(trajectory: Trajectory) {
    this.selectedTrajectory = trajectory;
    console.log('Selected trajectory: ' + this.selectedTrajectory);

    this.repaint();
  }

  private isConnectionSelected(fromCourse: number, toCourse: number) {
    let courses = this.selectedTrajectory.courses;

    for(let i = 0; i< courses.length - 1; ++i){
      let from = courses[i].id;
      let to = courses[i + 1].id;

      if(from == fromCourse && to == toCourse){
        return true;
      }
    }

    return false;
  }
}