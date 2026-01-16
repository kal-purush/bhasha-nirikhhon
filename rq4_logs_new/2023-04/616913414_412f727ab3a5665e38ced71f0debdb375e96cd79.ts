import { TMeta } from "@/services/types";

export type TSubject = {
    id: string,
    name: string,
    subject_code: string,
    degree: string,
    level: number,
    teacher_id: string,
    indicator: string,
    study_experience: string,
    teaching_materials: string,
    basic_competencies: string,
    tools_needed: string,
    scoring: string,
    description: string,
    thumbnail: string,
    credit: number,
    slug: string,
    subject_id: string
};

export type TSubjectResponse = TMeta<TSubject>;


  