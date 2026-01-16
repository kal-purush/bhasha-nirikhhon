// CLASS EXAMPLE

class Student {

  private _studentID:string;

  constructor(studentID:string) {
    this._studentID = studentID;
  }

  getStudentIDCohort():string {
    var cohort:string = 'No cohort';
    switch(this._studentID.substr(0, 2)) {
      case '33':
        cohort = 'Freshman';
        break;
      case '34':
        cohort = 'Sophomore';
        break;
      default:
        cohort = 'No cohort';
    }
    return cohort;
  }
}

var student = new Student('34-132456');
var studentCohort = student.getStudentIDCohort();
console.log('studentCohort: ' + studentCohort);