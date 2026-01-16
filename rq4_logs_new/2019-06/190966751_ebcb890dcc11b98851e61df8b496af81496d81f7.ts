import { Injectable } from '@angular/core';

interface Reference {
    description ?: string;          // How reference relates to module
    referenceName ?: string;        // Harvard style, http://www.citethisforme.com/
    link ?: string;                 // URL of reference
}

// Data for each module, usable across app.
export interface ModuleMetadata {
    title: string;                              // Title of module displayed in app
    id: string;                                 // unique id
    shortDescription?: string;                      // Description of module
    longDescription?: string;                  // Long description (shown in module viewer)
    hide?: boolean;                             // Whether module will be hidden
    references?: Reference[];                    // Reference data
}

const moduleLibrary: ModuleMetadata[] = [
    {   title: 'Lethal Dose',
        id: 'anaesthetics',
        shortDescription: 'Calculate max dose for drugs.'
    },
    {
        title: 'Procedure Outcomes',
        id: 'success-rates',
        shortDescription: 'Survival and failure rates of procedures.',
        references: [
            {description: 'Resin bonded bridge survival rates',
             referenceName: `Balasubramaniam, G. (2017). Predictability
             of resin bonded bridges – a systematic review. British Dental Journal, 222(11), pp.849-858.`,
            link: 'https://doi.org/10.1038/sj.bdj.2017.497'},
            {description: 'Implant survival rates',
            referenceName: `Howe, M., Keys, W. and Richards, D. (2019). Long-term (10-year) dental implant
            survival: A systematic review and sensitivity meta-analysis. Journal of Dentistry, 84, pp.9-21.`,
            link: 'https://doi.org/10.1016/j.jdent.2019.03.008'}
        ]
    },
    {
        title: 'Tooth classification and Prognosis',
        id: 'prognosis',
        shortDescription: 'Individual tooth classification and prognosis',
        longDescription: `Each tooth is evaluated for each of the 4 criteria independently. The level
                        of severity is evaluated both based on the presented condition and with consideration
                        to the foreseen tooth status after caries removal. The single most severe of these
                        criteria determines the tooth’s class.
                        <br /><br />
                        Anatomic risk factors and/or iatrogenic compromising factors may result in a drop of a
                        class for an individual tooth. More than 2 such findings in a tooth may require a
                        further drop in class
                        <br /><br />
                        Patient-level risk factors may result in a decreased prognosis for the dentition.
                        Therefore, a drop of 1 class for all teeth is suggested when considerable patient-level
                        risk factors are diagnosed. Patient-level risk factors are reassessed over time. A decrease
                        in class should be considered when no change in modifiable risk factors is observed, or when
                        significant nonmodifiable factors are present. An increase in class should be considered when
                        an obvious change in modifiable risk factors is observed.
                        `,
        references: [
            {description: 'Classification algorithm',
            referenceName: `Samet, Nachum & Jotkowitz, Anna. (2009). Classification and prognosis evaluation
            of individual teeth – a comprehensive approach. Quintessence international (Berlin, Germany : 1985).
            40. 377-87. `,
            link: 'https://www.researchgate.net/publication/26652014_Classification_and_prognosis_evaluation_of_\
            individual_teeth_-_a_comprehensive_approach'}
        ]
    }
];

@Injectable({providedIn: 'root'})
export class ModuleDataService {
    constructor() { }

    getModuleMetadata(id: string): ModuleMetadata {
        return(getModuleMetadata(id));
    }

    getModuleLibrary() {
        return moduleLibrary;
    }
  }

export function getModuleMetadata(id: string): ModuleMetadata {
    return(moduleLibrary.filter((module) => module.id === id)[0]);
}