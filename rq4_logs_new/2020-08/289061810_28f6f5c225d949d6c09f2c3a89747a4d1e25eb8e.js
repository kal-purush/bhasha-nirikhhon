import React from 'react';
import { IonTabButton, IonIcon, IonLabel } from '@ionic/react';

import config from '../config/config';

export const publicTabsRender = () => tabsRender(config.navigation.tabs.public);

export const privateTabsRender = () => tabsRender(config.navigation.tabs.private);

export const tabsRender = tabs => tabs.map(tab => (
    <IonTabButton tab={ tab.id } href={ `/${ tab.id }` } key={ tab.id }>
        <IonIcon icon={ tab.icon } />
        <IonLabel>{ tab.label }</IonLabel>
    </IonTabButton>
));