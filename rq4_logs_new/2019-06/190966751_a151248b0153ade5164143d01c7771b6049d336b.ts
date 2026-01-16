import { Injectable } from '@angular/core';

interface ProcedureStat {
    type: 'Survival' | 'Success';
    years: number;
    percentage: number;
    note ?: string;
}

interface ToothPrognosis {
    category: string;
    A: string;
    B: string;
    C: string;
    D: string;
    X: string;
}

const toothPrognosisList: ToothPrognosis[] = [
    {
        category: 'Periodontal health and alveolar support',
        A: `80% to 100% bone support. Can be easily maintained.`,
        B: `50% to 80% bone support, which can be well maintained with rigorous periodontal and
            maintenance therapy. Vertical defects or furcations that can be periodontally treated
            to become easily cleansable or treated predictably with regenerative therapy. Molars
            are at higher risk than single-rooted teeth.`,
        C: `30% to 50% remaining bone support. No ongoing acute outbreaks, but maintaining
            cleansability is alveolar support difficult. Periodontal therapy and a thorough
            maintenance program will enable the tooth to be maintained for an acceptable period
            of time.`,
        D: `A tooth with < 30% bone support, and/or one that cannot be cleansed or maintained well
            and has alveolar support evidence of active periodontal disease.`,
        X: `A tooth with < 30% bone support and cannot be cleansed or maintained without acute
            outbreaks of alveolar support periodontal infection.`
    },
    {
      category: 'Remaining tooth structure',
      A: `80% to 100% remaining sound coronal tooth structure. Can be easily restored.`,
      B: `50% to 80% remaining sound coronal tooth structure. Involved restorative procedures
          result in no infringement of biologic width, adequate ferrule, or good crown-root ratio and
          would minimally affect adjacent structures (if at all).`,
      C: `30% to 50% remaining sound coronal tooth structure. Or a tooth with so little tooth structure
          that achieving adequate ferrule would result in compromising the crown-root ratio to some
          extent, and/or may affect adjacent structures.`,
      D: `A tooth with < 30% sound tooth structure, or one in which the extent of the lost tooth structure
          does not enable a good ferrule to be achieved without totally compromising the support of the
          adjacent tooth structures or crown-root ratio.`,
      X: `No remaining supragingival sound coronal tooth structure. Loss of tooth structure deep into the
          root dentin/canals.`
  },
  {
    category: 'Endodontic condition',
    A: `A tooth that can receive a straightforward primary endodontic treatment, or already has good
        endodontic therapy`,
    B: `50% to 80% bone support, which can be well maintained with rigorous periodontal and
        maintenance therapy. Vertical defects or furcations that can be periodontally treated
        to become easily cleansable or treated predictably with regenerative therapy. Molars
        are at higher risk than single-rooted teeth.`,
    C: `30% to 50% remaining sound coronal tooth structure. Or a tooth with so little tooth structure
        that achieving adequate ferrule would result in compromising the crown-root ratio to some extent,
        and/or may affect adjacent structures.`,
    D: `A tooth with a failing endodontic treatment that cannot predictably be retreated`,
    X: `A vertical root fracture,3,68 or a tooth that has been retreated several times endodontically
        and/or surgically without resolution.`
},
{
  category: 'Occlusal plane and tooth position',
  A: `A tooth that is in the correct occlusal plane and/or position, or one that is slightly deviated from
      ideal and may require minimal enameloplasty.`,
  B: `A tooth that is out of the occlusal plane and can be adjusted so that it functions within the correct
      occlusal plane. Such a tooth may require additional treatment to seal exposed dentin.`,
  C: `A tooth that is out of the occlusal plane and requires multiple procedures to function within the occlusal
      plane`,
  D: `A tooth so severely out of the occlusal plane or severely tilted that after extensive treatment will
      exhibit position reduced crown-root ratio, which will prevent it from serving as a long-term unit in
      the arch. Or a tooth whose position impacts the health of the adjacent structures.`,
  X: `A tooth so far super-erupted or tilted out of the occlusal plane that it cannot be restored into
      correct function, or would interfere with the restoration of that arch or the restoration of the
      opposing arch.`
},
];

const prognosisInterpretation: ToothPrognosis = {
    category: 'Interpretation',
    A: `A tooth in this category is one that is considered to have a <b>good prognosis</b>. Such a tooth is
        assumed to have minimal risk of being lost in the foreseen future`,
    B: `A tooth in this category has a <b>fair</b> prognosis such that treatment outcome is considered
        predictable. Such a tooth poses a low risk of being lost in the foreseen future`,
    C: `A tooth in this category is one that has one or more problems and can be treated and maintained, but
        its prognosis remains <b>questionable</b>. Such a tooth has a medium risk of being lost`,
    D: `This category is for a <b>compromised</b> tooth that has a high risk of being lost. This includes those
        teeth that have no active pathologic conditions requiring immediate extraction, but it may not be in
        the patient’s best interest to invest in such a tooth. Since there is no obvious indication for
        extraction, external factors influencing the overall case and patient factors will play a major role
        in determining how to approach such a tooth.`,
    X: `A tooth in this category is <b>nonsalvageable</b> and is indicated for extraction. Such teeth cannot be
        restored or present pathologies that currently dentistry does not have a solution for. These include
        teeth that may pose risk to the patient’s health.`
};

@Injectable({providedIn: 'root'})
export class ToothPrognosisService {
  constructor() { }

  getToothPrognosisList() {
    return (toothPrognosisList);
  }

  getPrognosisInterpretation(toothClass: string) {
    return prognosisInterpretation[toothClass];
  }
}