interface Reference {
    description ?: string;
    referenceName ?: string;
    link ?: string;
}

// Data for each module, usable across app.
class ModuleMetadata {
    title: string;                              // Title of module displayed in app
    id: string;                                 // unique id
    shortDescription ?: string;                      // Description of module
    hide ? = false;                             // Whether module will be hidden
    longDescription ?: string;                  // Long description (shown in module viewer)
    references ?: Reference[];                    // Reference data
}

// Having a shared Module class allows the module viewer to access commonly used information
export class Module {
    metadata: ModuleMetadata;                   // Simple information for Module
    inViewer = false;                           // Whether the module has been loaded in module viewer
}

export const moduleLibrary: ModuleMetadata[] = [
    {   title: 'Lethal Dose',
        id: 'anaesthetics',
        shortDescription: 'Calculate max dose for drugs.'
    },
    {
        title: 'Procedure Outcomes',
        id: 'success-rates',
        shortDescription: 'Survival and failure rates of procedures.',
        longDescription: 'Test',
        references: [
            {description: 'Resin bonded bridge data',
             referenceName: `Balasubramaniam, G. (2017). Predictability 
             of resin bonded bridges – a systematic review. British Dental Journal, 222(11), pp.849-858.`,
            link: 'https://doi.org/10.1038/sj.bdj.2017.497'},
            {description: 'Description', referenceName: 'Reference Name', link: 'Link'},
            {description: 'Description 2', referenceName: 'Reference Name 2', link: 'Link 2'}
        ]
    },
    {
        title: 'Module Viewer Blank (testing)',
        id: 'module',
        shortDescription: 'Module Viewer blank'
    },
    {
        title: 'Module Viewer Lethal Dose (testing)',
        id: 'module/anaesthetics',
        shortDescription: 'Module Viewer with lethal dose'
    },
    {
        title: 'Module Viewer Procedure Outcomes (testing)',
        id: 'module/success-rates',
        shortDescription: 'Module Viewer with procedure outcomes'
    }
];

export function getModuleMetadata(id: string): ModuleMetadata {
    return(moduleLibrary.filter((module) => module.id === id)[0]);
}