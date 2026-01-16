import { ISubject } from 'types/entities/subject/ISubject';
import { ICreateSubject } from 'types/entities/subject/ICreateSubject';
import { ApiService } from '../ApiService';

class SubjectApiDomainService {
  createSubject(payload: ICreateSubject) {
    return ApiService.getInstance().postJson<any>('/Subject/add', payload);
  }

  getSubjectsByEducationSet(educationSetId: string) {
    return ApiService.getInstance().getJson<ISubject[]>(`/Subject/EducationSet/${educationSetId}`);
  }
}

const subjectApiDomainService = new SubjectApiDomainService();

export { subjectApiDomainService };