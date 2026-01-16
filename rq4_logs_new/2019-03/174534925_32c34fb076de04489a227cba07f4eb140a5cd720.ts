import { Component, ViewChild } from '@angular/core';

import { Events, MenuController, Nav, Platform } from 'ionic-angular';
import { SplashScreen } from '@ionic-native/splash-screen';

import { Storage } from '@ionic/storage';

import { AboutPage } from '../pages/about/about';
import { AccountPage } from '../pages/account/account';
import { LoginPage } from '../pages/login/login';
import { MapPage } from '../pages/map/map';
import { SignupPage } from '../pages/signup/signup';
import { TabsPage } from '../pages/tabs-page/tabs-page';
import { TutorialPage } from '../pages/tutorial/tutorial';
import { SchedulePage } from '../pages/schedule/schedule';
import { SpeakerListPage } from '../pages/speaker-list/speaker-list';
import { SupportPage } from '../pages/support/support';
import { SermonsPage } from '../pages/sermons/sermons';
import { WelcomePage } from '../pages/welcome/welcome';
//import { SeriesPage } from '../pages/series/series';
//import { EpisodePage } from '../pages/episode/episode';
import { EventsPage } from '../pages/events/events';
//import { EventDetailsPage } from '../pages/event-details/event-details';
import { BiblePage } from '../pages/bible/bible';
import { NewsPage } from '../pages/news/news';
//import { NewsArticlePage } from '../pages/news-article/news-article';
//import { LeadersPage } from '../pages/leaders/leaders';
import { MorePage } from '../pages/more/more';
//import { BranchesPage } from '../pages/branches/branches';
//import { AboutUsPage } from '../pages/about-us/about-us';

//import { SocialPage } from '../pages/social/social';
//import { MusicPage } from '../pages/music/music';
//import { TithingPage } from '../pages/tithing/tithing';
//import { ConnectPage } from '../pages/connect/connect';
//import { MinistriesPage } from '../pages/ministries/ministries';
//import { LocationMapPage } from '../pages/location-map/location-map';

import { OnlinegivingPage } from './../pages/onlinegiving/onlinegiving';
import { DevotionPage } from './../pages/devotion/devotion';
import { DevotionalPage } from './../pages/devotional/devotional';
import { DevotionaldetailsPage } from './../pages/devotionaldetails/devotionaldetails';
import { WeeklydeclarationPage } from './../pages/weeklydeclaration/weeklydeclaration';
import { WeeklydeclarationdetailsPage } from './../pages/weeklydeclarationdetails/weeklydeclarationdetails';


import { GivingPage } from './../pages/giving/giving';
import { StreamPage } from './../pages/stream/stream';

import { OneSignal } from '@ionic-native/onesignal';

import { ConferenceData } from '../providers/conference-data';
import { UserData } from '../providers/user-data';
import firebase from 'firebase';

export interface PageInterface {
  title: string;
  name: string;
  component: any;
  icon: string;
  logsOut?: boolean;
  index?: number;
  tabName?: string;
  tabComponent?: any;
}

@Component({
  templateUrl: 'app.template.html'
})
export class ConferenceApp {
  // the root nav is a child of the root app component
  // @ViewChild(Nav) gets a reference to the app's root nav
  @ViewChild(Nav) nav: Nav;

  // List of pages that can be navigated to from the left menu
  // the left menu only works after login
  // the login page disables the left menu
  appPages: PageInterface[] = [
    { title: 'Welcome', name: 'TabsPage', component: TabsPage, tabComponent: WelcomePage, index: 0, icon: 'home' },
    { title: 'Sermons', name: 'TabsPage', component: TabsPage, tabComponent: SermonsPage, index: 1, icon: 'mic' },
    { title: 'Online Giving', name: 'TabsPage', component: TabsPage, tabComponent: GivingPage, index: 2, icon: 'card' },
    { title: 'Daily Devotional', name: 'TabsPage', component: TabsPage, tabComponent: DevotionPage, index: 3, icon: 'book' },
    //{ title: 'Events', name: 'EventsPage', component: EventsPage, icon: 'information-circle' },
   //{ title: 'Weekly Declaration', name: 'WeeklydeclarationPage', component: WeeklydeclarationPage, icon: 'information-circle' },
    { title: 'More', name: 'TabsPage', component: TabsPage, tabComponent: MorePage, index: 4, icon: 'apps' }
  ];
 // loggedInPages: PageInterface[] = [
   // { title: 'Account', name: 'AccountPage', component: AccountPage, icon: 'person' },
   // { title: 'Support', name: 'SupportPage', component: SupportPage, icon: 'help' },
    
    //{ title: 'Series', name: 'SeriesPage', component: SeriesPage, icon: 'help' },
   // { title: 'Logout', name: 'TabsPage', component: TabsPage, icon: 'log-out', logsOut: true }
  //];
  //loggedOutPages: PageInterface[] = [
   // { title: 'Login', name: 'LoginPage', component: LoginPage, icon: 'log-in' },
    //{ title: 'Support', name: 'SupportPage', component: SupportPage, icon: 'help' },
    //{ title: 'Sermons', name: 'SermonsPage', component: SermonsPage, icon: 'help' },
    //{ title: 'Events', name: 'EventsPage', component: EventsPage, icon: 'help' },
    //{ title: 'Bible', name: 'BiblePage', component: BiblePage, icon: 'help' },
    //{ title: 'News', name: 'NewsPage', component: NewsPage, icon: 'help' },
   // { title: 'More', name: 'MorePage', component: MorePage, icon: 'help' },
    //{ title: 'Signup', name: 'SignupPage', component: SignupPage, icon: 'person-add' }
  //];
  rootPage: any;

  constructor(
    public events: Events,
    public userData: UserData,
    public menu: MenuController,
    public platform: Platform,
    public confData: ConferenceData,
    public storage: Storage,
    public splashScreen: SplashScreen,
    public oneSignal:OneSignal
  ) {
      
      //Firebase Configuration
    firebase.initializeApp({
     apiKey: "AIzaSyD6YE2ArIp4PNO4OTZh3BDIgjYjrNyFNuE",
    authDomain: "troi-5e92f.firebaseapp.com",
    databaseURL: "https://troi-5e92f.firebaseio.com",
    projectId: "troi-5e92f",
    storageBucket: "troi-5e92f.appspot.com",
    messagingSenderId: "351782808779"
    });

    // Check if the user has already seen the tutorial
    this.storage.get('hasSeenTutorial')
      .then((hasSeenTutorial) => {
        if (hasSeenTutorial) {
          this.rootPage = TabsPage;
        } else {
          this.rootPage = TabsPage;
        }
        this.platformReady()

        this.initApp();

      });

    // load the conference data
    confData.load();

    // decide which menu items should be hidden by current login status stored in local storage
    this.userData.hasLoggedIn().then((hasLoggedIn) => {
      this.enableMenu(hasLoggedIn === true);
    });
    this.enableMenu(true);

    this.listenToLoginEvents();
  }






 













  openPage(page: PageInterface) {
    let params = {};

    // the nav component was found using @ViewChild(Nav)
    // setRoot on the nav to remove previous pages and only have this page
    // we wouldn't want the back button to show in this scenario
    if (page.index) {
      params = { tabIndex: page.index };
    }

    // If we are already on tabs just change the selected tab
    // don't setRoot again, this maintains the history stack of the
    // tabs even if changing them from the menu
    if (this.nav.getActiveChildNavs().length && page.index != undefined) {
      this.nav.getActiveChildNavs()[0].select(page.index);
    // Set the root of the nav with params if it's a tab index
  } else {
      this.nav.setRoot(page.name, params).catch((err: any) => {
        console.log(`Didn't set nav root: ${err}`);
      });
    }

    if (page.logsOut === true) {
      // Give the menu time to close before changing to logged out
      this.userData.logout();
    }
  }

  openTutorial() {
    this.nav.setRoot(TutorialPage);
  }

  listenToLoginEvents() {
    this.events.subscribe('user:login', () => {
      this.enableMenu(true);
    });

    this.events.subscribe('user:signup', () => {
      this.enableMenu(true);
    });

    this.events.subscribe('user:logout', () => {
      this.enableMenu(false);
    });
  }

  enableMenu(loggedIn: boolean) {
    this.menu.enable(loggedIn, 'loggedInMenu');
    this.menu.enable(!loggedIn, 'loggedOutMenu');
  }

  platformReady() {
    // Call any initial plugins when ready
    this.platform.ready().then(() => {
      this.splashScreen.hide();
    });
  }




  isActive(page: PageInterface) {
    let childNav = this.nav.getActiveChildNavs()[0];

    // Tabs are a special case because they have their own navigation
    if (childNav) {
      if (childNav.getSelected() && childNav.getSelected().root === page.tabComponent) {
        return 'primary';
      }
      return;
    }

    if (this.nav.getActive() && this.nav.getActive().name === page.name) {
      return 'primary';
    }
    return;
  }




initApp(){

    // this.statusBar.overlaysWebView(true);
    //this.statusBar.backgroundColorByHexString('#ffffff');


    this.oneSignal.startInit('928f9393-947c-4fd7-9fdd-5fdbfb9fd7fa', '716893243943');
    
    this.oneSignal.setSubscription(true);
    //this.oneSignal.inFocusDisplaying(this.oneSignal.OSInFocusDisplayOption.InAppAlert);

    this.oneSignal.handleNotificationReceived().subscribe(() => {
      // do something when notification is received
    });

    this.oneSignal.handleNotificationOpened().subscribe(() => {
      // do something when a notification is opened
    });

    this.oneSignal.endInit();

    //this.ga.startTrackerWithId('UA-85xxxxx7-3')
    //.then(() => {

      //this.ga.trackView('App V3 Started');

      // })
      //.catch(e => console.log('Error starting GoogleAnalytics', e));
    }














}