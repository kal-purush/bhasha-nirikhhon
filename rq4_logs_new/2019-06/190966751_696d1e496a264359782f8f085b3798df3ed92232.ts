export class ModuleData {
    title: string;
    page: string;
    location: string;
}

export const moduleLibrary = new Map<string, ModuleData>([
    ['anaesthetics', {
        title: 'Lethal Dose (Test text 1)',
        page: 'anaesthetics',
        location: './modules/anaesthetics/anaesthetics.module#AnaestheticsPageModule'
    }
    ],
    ['anaesthetics2', {
        title: 'Lethal Dose (Test text 2)',
        page: 'anaesthetics',
        location: './modules/anaesthetics/anaesthetics.module#AnaestheticsPageModule'
    }
    ]
]);
