/**
 * Learn more about deep linking with React Navigation
 * https://reactnavigation.org/docs/deep-linking
 * https://reactnavigation.org/docs/configuring-links
 */

import { LinkingOptions } from '@react-navigation/native';
import { createURL } from 'expo-linking';

import { Weaken } from '../Hooks/CHK003TypeScript';

import {
	ItineraryStackParamList,
	ItineraryTopTabParamList,
	PlaceStackParamList,
	RootStackParamList,
	RootTabParamList,
} from './NavigationInterface';

//  {
//     prefixes: [ENV.HOST_NAME_WEB],
// }

const ItineraryTopTabScreenProps: Weaken<ItineraryTopTabParamList, keyof ItineraryTopTabParamList> = {
	ItineraryEdit: 'ItineraryEdit',
	ItineraryPreview: 'ItineraryPreview',
};

const ItineraryStackScreens: Weaken<ItineraryStackParamList, keyof ItineraryStackParamList> = {
	ItineraryCover: 'ItineraryCover',
	ItineraryTopTabNavigator: {
		screens: ItineraryTopTabScreenProps,
	},
	EditPlan: 'EditPlan',
	HelloSpelieve: 'HelloSpelieve',
};

const PlaceStackScreens: Weaken<PlaceStackParamList, keyof PlaceStackParamList> = {
	PPA001Places: 'PPA001Places',
	PPA002Place: 'PPA002Place',
};

const RootTabScreens: Weaken<RootTabParamList, keyof RootTabParamList> = {
	Itinerary: {
		screens: ItineraryStackScreens,
	},
	Place: {
		screens: PlaceStackScreens,
	},
};

const RootStackScreens: Weaken<RootStackParamList, keyof RootStackParamList> = {
	Root: {
		screens: RootTabScreens,
	},
	Modal: 'modal',
	NotFound: '*',
};

const config = {
	screens: RootStackScreens,
};

export const LinkingConfiguration: LinkingOptions<RootStackParamList> = {
	prefixes: [createURL('/')],
	config,
};