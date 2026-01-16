import { fullCopyBusiness } from '../../../../src/api/processors/businesse.proc';
import { fullCopyCampaign } from '../../../../src/api/processors/campaign.proc';
import { fullCopyFile } from '../../../../src/api/processors/file.proc';
import { fullCopyInternship } from '../../../../src/api/processors/internship.proc';
import { fullCopyInternshipType } from '../../../../src/api/processors/internship.type.proc';
import { fullCopyMentoringProposition } from '../../../../src/api/processors/mentoring.proposition.proc';
import { fullCopyMentor } from '../../../../src/api/processors/mentor.proc';
import { fullCopyStudent } from '../../../../src/api/processors/student.proc';

import {
    defaultCampaigns,
    defaultBusiness,
    defaultFiles,
    defaultInternships,
    defaultInternshipTypes,
    defaultMentoringPropositions,
    defaultMentors,
    defaultStudents,
} from '../../../../__mocks__/mockData';

describe('Business', () => {
    it('fullCopyBusiness_business_same', () => {
        expect(fullCopyBusiness(defaultBusiness())).toEqual(defaultBusiness());
    });

    it('fullCopyBusiness_empty_undefined', () => {
        expect(fullCopyBusiness({} as any)).toBe(undefined);
    });
});

describe('Campaign', () => {
    it('fullCopyCampaign_campaign_same', () => {
        const PROCESSED = defaultCampaigns();
        delete PROCESSED.category;

        expect(fullCopyCampaign(defaultCampaigns())).toEqual(PROCESSED);
    });

    it('fullCopyCampaign_empty_undefined', () => {
        expect(fullCopyCampaign({} as any)).toBe(undefined);
    });
});

describe('File', () => {
    it('fullCopyFile_file_same', () => {
        expect(fullCopyFile(defaultFiles())).toEqual(defaultFiles());
    });

    it('fullCopyFile_empty_undefined', () => {
        expect(fullCopyFile({} as any)).toBe(undefined);
    });
});

describe('Internship', () => {
    it('fullCopyInternship_internship_same', () => {
        expect(fullCopyInternship(defaultInternships())).toEqual(defaultInternships());
    });

    it('fullCopyInternship_empty_undefined', () => {
        expect(fullCopyInternship({} as any)).toBe(undefined);
    });
});

describe('InternshipType', () => {
    it('fullCopyInternshipType_internshipType_same', () => {
        expect(fullCopyInternshipType(defaultInternshipTypes())).toEqual(defaultInternshipTypes());
    });

    it('fullCopyInternshipType_empty_undefined', () => {
        expect(fullCopyInternshipType({} as any)).toBe(undefined);
    });
});

describe('MentoringProposition', () => {
    it('fullCopyMentoringProposition_mentoringProposition_same', () => {
        expect(fullCopyMentoringProposition(defaultMentoringPropositions())).toEqual(
            defaultMentoringPropositions(),
        );
    });

    it('fullCopyMentoringProposition_empty_undefined', () => {
        expect(fullCopyMentoringProposition({} as any)).toBe(undefined);
    });
});

describe('Mentor', () => {
    it('fullCopyMentor_mentor_same', () => {
        expect(fullCopyMentor(defaultMentors())).toEqual(defaultMentors());
    });

    it('fullCopyMentor_empty_undefined', () => {
        expect(fullCopyMentor({} as any)).toBe(undefined);
    });
});

describe('Student', () => {
    it('fullCopyStudent_student_same', () => {
        expect(fullCopyStudent(defaultStudents())).toEqual(defaultStudents());
    });

    it('fullCopyStudent_empty_undefined', () => {
        expect(fullCopyStudent({} as any)).toBe(undefined);
    });
});