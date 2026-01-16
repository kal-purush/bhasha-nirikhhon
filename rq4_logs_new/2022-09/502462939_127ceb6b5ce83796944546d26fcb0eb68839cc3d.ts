export type PropsParticipantes = {
  userId: number;
  name: string;
  username: string;
  photo: string;
  role: string;
  email: string;
}[];

export type PropsProfessor = {
  userId: number;
  name: string;
  username: string;
  photo: string;
  role: string;
  email: string;
};

export type PropsTurma = {

      name: string;
      professorId: number;
      id: number;
      cronograma: null;
      inicio: any;
      fim: any;
      isFinished: number;
      faltasPermitidas: number;
      mediaMinima: number;
   
  }