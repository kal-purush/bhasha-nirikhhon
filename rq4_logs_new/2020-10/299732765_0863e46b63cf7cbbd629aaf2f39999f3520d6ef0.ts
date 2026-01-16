import Student from '../models/Student';

interface CreateStudentDTO {
  ra: string;
  name: string;
  email: string;
  cpf: string;
}

class StudentsRepository {
  private students: Student[];

  constructor() {
    this.students = [];
  }

  public findByEmail(email: string): Student | null {
    const findStudent = this.students.find(student => student.email === email);

    return findStudent || null;
  }

  public create({ ra, name, email, cpf }: CreateStudentDTO): Student {
    const student = { ra, name, email, cpf };

    this.students.push(student);

    return student;
  }
}

export default StudentsRepository;