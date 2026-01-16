import Shepherd from 'shepherd.js';

const visualTour: Shepherd.Tour = new Shepherd.Tour({
    useModalOverlay: true,
    defaultStepOptions: {
      classes: 'shadow-md bg-purple-dark',
      scrollTo: { behavior: 'smooth', block: 'center' },
      cancelIcon: {
        enabled: true
      },
      buttons: [
        {
          text: 'Previous',
          action: () => visualTour.back()
        },
        {
          text: 'Next',
          action: () => visualTour.next()
        },
      ]
    },
});

// load from local storage if the user has already seen the tour
const alreadySeenTour = localStorage.getItem('tour') === 'true';

// function that saves that the user has seen the tour in local storage
function userHasSeenTour() {
    localStorage.setItem('tour', 'true');
}

visualTour.addSteps([
    {
        title: 'Welcome to ORCA',
        text: 'ORCA is a dashboard for visualizing and analyzing data of the organ donation process.<br>In this tour, we will show you the main features of the dashboard.',
        buttons: [
            {
                text: 'Do not show again',
                action: () => {
                    visualTour.cancel();
                    userHasSeenTour();
                }
            },
            {
                text: 'Skip',
                action: () => visualTour.cancel()
            },
            {
                text: 'Start',
                action: () => visualTour.next()
            },
        ]
    },
    {
        title: 'Grid',
        text: 'This is the grid. It is the main component of the dashboard. It consists of tiles, which are visualizations of data.',
        attachTo: {
            element: '.vue-grid-layout',
            on: 'top'
        },
    },
    {
        title: 'Tiles',
        text: 'Tiles are visualizations of data. They can be moved by dragging them around the grid. You can also resize them by dragging the bottom right corner. Feel free to try it out!',
        attachTo: {
            element: '#vue-tile-0',
            on: 'right'
        },
    },
    {
        title: 'Distribution Visualization',
        text: 'This tile shows the first type of visualization: a distribution. To be exact, currently it shows the distribution of genders in the dataset.' +
            'You can change the disaggregation attribute by editing the tile.<br> Lets start by changing this visualization to show the distribution of ages in the dataset. Click on the edit button in the top right corner of the tile.',
        attachTo: {
            element: '#vue-tile-0',
            on: 'right'
        },
        advanceOn: { selector: '#vue-tile-0 .flex-1:nth-child(3)>.tile-btn', event: 'click' },
        buttons: [],
    },
    {
        title: 'Disaggregation Attribute',
        text: 'This is the disaggregation attribute. It is used to determine the distribution. You can change it by clicking on the dropdown menu and selecting a different attribute.',
        attachTo: {
            element: 'input#change-title+div.p-dropdown',
            on: 'right'
        },
        advanceOn: { selector: 'input#change-title+div.p-dropdown', event: 'click' },
        buttons: [],
    },
    {
        title: 'Select a Disaggregation Attribute',
        text: 'Select the age attribute.',
        attachTo: {
            element: 'ul.p-dropdown-items',
            on: 'right'
        },
        advanceOn: { selector: 'ul.p-dropdown-items>li:nth-child(2)', event: 'click' },
        buttons: [],
    },
    {
        title: 'Tile Title',
        text: 'This is the title of the tile. You can change it by clicking on the text and typing a new title.',
        attachTo: {
            element: 'input#change-title',
            on: 'right'
        },
    },
    {
        title: 'Save Changes',
        text: 'You can save the changes you made by clicking on the save button.',
        attachTo: {
            element: 'div.p-dialog-footer>button.primary',
            on: 'right'
        },
        advanceOn: { selector: 'div.p-dialog-footer>button.primary', event: 'click' },
    },
    {
        title: 'Age Distribution',
        text: 'The tile now shows the distribution of ages in the dataset. You can see that the majority of the patients are between 50 and 80 years old.<br>' +
            'Let\'s investigate how the age distribution looks for female patients. To add filters click the "Add Filter" button in the bottom center of the tile.',
        attachTo: {
            element: '#vue-tile-0',
            on: 'right'
        },
        advanceOn: { selector: '#vue-tile-0 .tile-footer button', event: 'click' },
        buttons: [],
    },
    {
        title: 'Add Filter',
        text: 'First start by selecting the attribute you want to filter by. The attributes are grouped into two categories: "Patient" and "Process" attributes.<br>' +
            'Under the patient group you can find all extracted attributes of the patient. Under the process group you can find all supported attributes of the process.<br>' +
            'Or you can search for an attribute by typing its name into the search bar.<br>' +
            'Select the "Gender" attribute to add your first filter',
        attachTo: {
            element: '.p-overlaypanel',
            on: 'right'
        },
        advanceOn: { selector: '.p-overlaypanel .p-listbox-list-wrapper>ul', event: 'click' },
        buttons: [],
    },
    {
        title: 'Edit filter',
        text: 'By default all added filters are set to "is not empty". You can change this by clicking on the filter.',
        attachTo: {
            element: '#vue-tile-0 .tile-footer',
            on: 'right'
        },
        advanceOn: { selector: '#vue-tile-0 .tile-footer .p-chip', event: 'click' },
        buttons: [],
    },
    {
        title: 'Filter operator',
        text: 'You can change the operator by clicking on the dropdown menu and selecting a different operator.',
        attachTo: {
            element: '.p-overlaypanel',
            on: 'right'
        },
        advanceOn: { selector: '.p-overlaypanel .p-dropdown', event: 'click' },
        buttons: [],
    },
    {
        title: 'Select operator',
        text: 'Select the "EQUALS" operator.',
        attachTo: {
            element: '.p-dropdown-panel .p-dropdown-items',
            on: 'right'
        },
        advanceOn: { selector: '.p-dropdown-panel .p-dropdown-items', event: 'click' },
        buttons: [],
    },
    {
        title: 'Filter value',
        text: 'You can change the value of the filter by clicking on the input field. For categorical attributes you can select a value from a dropdown menu. For numerical attributes you can type in a value.',
        attachTo: {
            element: '.p-overlaypanel .p-dropdown:nth-child(2)',
            on: 'right'
        },
        advanceOn: { selector: '.p-overlaypanel .p-dropdown:nth-child(2)', event: 'click' },
        buttons: [],
    },
    {
        title: 'Select filter value',
        text: 'As we want to see the age distribution for female patients, select the "F" (Female) value.',
        attachTo: {
            element: '.p-dropdown-panel .p-dropdown-items',
            on: 'right'
        },
        advanceOn: { selector: '.p-dropdown-panel .p-dropdown-items>li', event: 'click' },
    },
    {
        title: 'Filtered Age Distribution',
        text: 'Here we go! Now we see the distribution of ages among female patients.<br>' +
            'You can remove a filter by clicking on the "x" next to the filter.' +
            'Or you can add even more filters by clicking on the "Add Filter" button again. Those filters are combined with an "AND" operator.<br>' +
            'Last but not least you can download the visualization by clicking on the "Download" button in the top right corner of the tile.',
        attachTo: {
            element: '#vue-tile-0',
            on: 'right'
        },
    },
    {
        title: 'Global Filters',
        text: 'You can also add global filters. These filters are applied to all tiles. To add a global filter click on the "Add Global Filter" button in the top center of the dashboard.',
        attachTo: {
            element: '#global-filters',
            on: 'bottom'
        },
    },
    {
        title: 'Add a Tile',
        text: 'You can add a new tile by clicking on the "Add Tile" button in the top right corner of the dashboard.',
        attachTo: {
            element: '#add-tile-button',
            on: 'bottom'
        },
        advanceOn: { selector: '#add-tile-button', event: 'click' },
        buttons: [],
    },
    {
        title: 'Configure Tile',
        text: 'You can configure the tile by clicking on the "Configure Tile" button in the top right corner of the tile.',
        attachTo: {
            element: '.vue-grid-item:nth-last-child(3)',
            on: 'top'
        },
        advanceOn: { selector: '.vue-grid-item:nth-last-child(3) .flex-1:nth-child(3)>.tile-btn', event: 'click' },
        buttons: [],
    },
    {
        title: 'Select Visualization',
        text: 'You can select a visualization by clicking on the dropdown menu and selecting a visualization. Do not forget to also select a disaggregation attribute and to save the changes.',
        attachTo: {
            element: '.p-dialog.p-component.modal-window',
            on: 'right'
        },
        advanceOn: { selector: 'div.p-dialog-footer>button.primary', event: 'click' },
        buttons: [],
    },
    {
        title: 'Remove Tile',
        text: 'You can remove a tile by clicking on the "Remove Tile" button in the top right corner of the tile.',
        attachTo: {
            element: '.vue-grid-item:nth-last-child(3) .flex-1:nth-child(4)>.tile-btn',
            on: 'right'
        },
    },
    {
        title: 'End of Tour',
        text: 'This is the end of the tour. Please refer to the user manual for more and detailed information.<br>' +
            'If you want to see the tour again, you can click on the "Help" button in the top right corner of the dashboard.<br>' +
            'We hope you enjoy using ORCA!',
        buttons: [
            {
                text: 'Finish',
                action: () => {
                    visualTour.cancel();
                    userHasSeenTour();
                }
            },
        ]
    }
])

export { visualTour, alreadySeenTour};