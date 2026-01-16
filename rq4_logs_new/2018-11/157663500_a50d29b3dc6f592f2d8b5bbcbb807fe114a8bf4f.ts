import { Component, ViewChild } from "@angular/core";
import {
  Nav,
  Platform,
  Events,
  AlertController,
  ToastController,
  NavController
} from "ionic-angular";
import { StatusBar } from "@ionic-native/status-bar";
import { SplashScreen } from "@ionic-native/splash-screen";

/******  Menu Modules  ******/
import { SettingsProvider } from "../providers/settings";
import { AuthService } from "../services/auth-service";
import { OrderService } from "../services/order-service";
import { Network } from "@ionic-native/network";
import { Diagnostic } from "@ionic-native/diagnostic";
import { ConfigService } from "../services/config";
import { StorageService } from "../services/storage-service";
import { HelperService } from "../services/helper-service";
import { LoginPage } from "../pages/auth/login/login";
import { NewOrdersPage } from "../pages/new-orders/new-orders";
import { WaitingOrdersPage } from "../pages/waiting-orders/waiting-orders";
import { ActiveOrderPage } from "../pages/active-order/active-order";
import { NotAttachedOrdersPage } from "../pages/not-attached-orders/not-attached-orders";
import { ClosedOrdersPage } from "../pages/closed-orders/closed-orders";
import { SettingsPage } from "../pages/settings/settings";
import { MessagePage } from "../pages/message/message";
import { LocationTracker } from "../providers/location-tracker";
import { HockeyApp } from "ionic-hockeyapp";
import { AppVersion } from "@ionic-native/app-version";
import { Market } from "@ionic-native/market";
import { HelpPage } from "../pages/help/help";

@Component({
  templateUrl: "app.html"
})
export class MyApp {
  @ViewChild(Nav)
  nav: Nav;
  @ViewChild("content")
  navCtrl: NavController;

  rootPage: any = LoginPage;
  activePage: any;
  selectedTheme: String;
  toast: any;
  isNetworkAlertActive: boolean = false;

  pages: Array<{ title: string; component: any; icon: string }>;
  pagesQty: Array<{ qty: number }>;

  constructor(
    public platform: Platform,
    public statusBar: StatusBar,
    public splashScreen: SplashScreen,
    private authService: AuthService,
    private orderService: OrderService,
    private network: Network,
    private alertCtrl: AlertController,
    public toastCtrl: ToastController,
    private helperService: HelperService,
    private diagnostic: Diagnostic,
    private configService: ConfigService,
    public events: Events,
    private market: Market,
    private hockeyapp: HockeyApp,
    private locationTracker: LocationTracker,
    private storageService: StorageService,
    private appVersion: AppVersion,
    private settings: SettingsProvider
  ) {
    this.settings.getActiveTheme().subscribe(val => (this.selectedTheme = val));
    this.initializeApp();

    this.pages = [
      { title: "Новые", component: NewOrdersPage, icon: "menu_001.png" },
      {
        title: "Ожидают погрузки",
        component: WaitingOrdersPage,
        icon: "menu_002.png"
      },
      { title: "В работе", component: ActiveOrderPage, icon: "menu_003.png" },
      {
        title: "Не приложены документы",
        component: NotAttachedOrdersPage,
        icon: "menu_004.png"
      },
      {
        title: "Закрытые листы",
        component: ClosedOrdersPage,
        icon: "menu_005.png"
      },
      { title: "Сообщения", component: MessagePage, icon: "menu_006.png" },
      { title: "Настройки", component: SettingsPage, icon: "menu_007.png" },
      { title: "Помощь", component: HelpPage, icon: "help.png" }
    ];

    this.updateMenu();

    let tn = JSON.parse(localStorage.getItem("tn"));
    if (tn && tn.token && tn.token !== undefined && tn.token !== "") {
      this.activePage = this.pages[0];
      this.rootPage = NewOrdersPage;
    } else {
      this.rootPage = LoginPage; //HelpPage
    }

    this.events.subscribe("user:login", () => {
      setTimeout(() => {
        this.updateMenu();
      }, 0);
    });
  }

  setActivePage(page) {
    return page == this.activePage;
  }

  updateMenu() {
    this.pagesQty = [
      { qty: this.orderService.newOrdersQty },
      { qty: this.orderService.waitingOrdersQty },
      { qty: this.orderService.activeOrdersQty },
      { qty: this.orderService.notAttachedOrdersQty },
      { qty: this.orderService.historyOrdersQty },
      { qty: null },
      { qty: null },
      { qty: null }
    ];
  }

  initializeApp() {
    this.platform.ready().then(() => {
      this.statusBar.styleDefault();
      this.splashScreen.hide();

      this.helperService.oneSignalConf();
      this.hockeyapp.start(
        "08c35314eddb424888b48d7a481365e1",
        null,
        false,
        false
      );

      this.checkInternetConnection();

      this.authService.getAppVersion().then(
        res => {
          var str = res;
          var n = str.search("Current Version");

          var el = document.createElement("html");
          el.innerHTML = res;

          var list = el.getElementsByClassName("hAyfc")[3];
          console.log(res);
          var googlePlayVersion = list.getElementsByTagName("span")[1]
            .innerHTML;
          console.log("[googlePlayVersion]", googlePlayVersion);

          this.appVersion.getVersionNumber().then(
            res => {
              console.log("[app version]", res);
              if (res !== googlePlayVersion) {
                let appAlert = this.alertCtrl.create({
                  title: "Доступно новое обновление!",
                  message: "Открыть GooglePlay?",
                  buttons: [
                    {
                      text: "Отмена",
                      handler: () => { }
                    },
                    {
                      text: "Далее",
                      handler: () => {
                        this.market.open(this.authService.packageName);
                      }
                    }
                  ]
                });
                appAlert.present();
              }
            },
            error => {
              console.log("[error getting version]");
            }
          );
        },
        error => {
          console.log(error);
        }
      );

      this.locationTracker.getUserCurrentPosition();

      this.toast = this.toastCtrl.create({
        message: "Нет соединения с интернетом!",
        duration: 0,
        cssClass: "errorToast",
        position: "top"
      });

      setTimeout(() => {
        console.log(this.helperService.isNetworkConnected);
        if (!this.helperService.isNetworkConnected) {
          this.initOfflineData();
        } else {
          this.sendOfflineData();
        }
      }, 1000);
    });
  }

  initOfflineData() {
    this.storageService.getDataByKey("activeOrders").then(result => {
      this.orderService.activeOrders = JSON.parse(result);
      this.orderService.activeOrdersQty = this.orderService.activeOrders.length;
      this.updateMenu();
    });

    this.storageService.getDataByKey("newOrders").then(result => {
      this.orderService.newOrders = JSON.parse(result);
      this.orderService.newOrdersQty = this.orderService.newOrders.length;
      this.updateMenu();
    });

    this.storageService.getDataByKey("waitingOrders").then(result => {
      this.orderService.waitingOrders = JSON.parse(result);
      this.orderService.waitingOrdersQty = this.orderService.waitingOrders.length;
      this.updateMenu();
    });

    this.storageService.getDataByKey("notAttachedOrders").then(result => {
      this.orderService.notAttachedOrders = JSON.parse(result);
      this.orderService.notAttachedOrdersQty = this.orderService.notAttachedOrders.length;
      this.updateMenu();
    });

    this.storageService.getDataByKey("offlineDocuments").then(result => {
      if (result !== null) {
        this.helperService.offlinePhotos = JSON.parse(result);
      }
    });
  }

  checkInternetConnection() {
    console.log("init checkInternetConnection");
    this.network.onDisconnect().subscribe(() => {
      console.log("network was disconnected :-(");
      this.helperService.isNetworkConnected = false;
      this.orderService.isNetworkConnected = false;
      this.showNetworkAlert();
    });

    this.network.onConnect().subscribe(res => {
      console.log("network was connected :-)");
      this.authService.isNetworkConnected = true;
      this.helperService.isNetworkConnected = true;
      this.orderService.isNetworkConnected = true;

      this.sendOfflineData();
    });
    this.locationTracker.testCheckGps(null, false);
  }

  sendOfflineData() {
    this.storageService.getDataByKey("offlineData").then(result => {
      console.log("offlineData", JSON.parse(result));
      this.helperService.offlineData = JSON.parse(result);
      if (
        this.helperService.offlineData &&
        this.helperService.offlineData !== null &&
        this.helperService.offlineData.length > 0
      ) {
        this.helperService.offlineData = JSON.parse(result);
        this.orderService.sendOfflineData(this.helperService.offlineData).then(
          (res: any) => {
            console.log(res);
            this.helperService.offlineData = [];
            this.storageService.removeItem("offlineData");
            this.storageService.getDataByKey("offlineData").then(
              result => {
                let offData = [];
                this.storageService.storeOfflineStates("offlineData", offData);
              },
              error => {
                console.log(error);
              }
            );

            this.orderService.getNewOrders(true);
            this.orderService.getWaitingOrders(true);
            this.orderService.getActiveOrders(true);
          },
          (error: any) => {
            console.log(error);
            this.configService.presentToast(
              error.message,
              "error",
              "middle",
              false,
              "",
              3000
            );
          }
        );
      }
    });

    this.storageService.getDataByKey("offlineDocuments").then(result => {
      if (result !== null) {
        this.helperService.offlinePhotos = JSON.parse(result);

        this.orderService
          .sendPackDocuments(this.helperService.offlinePhotos)
          .then(
            res => {
              //send to server, inside success
              this.storageService.removeItem("offlineDocuments");
              this.storageService.getDataByKey("offlineDocuments").then(
                result => {
                  this.helperService.offlinePhotos = [];
                },
                error => { }
              );
            },
            error => {
              console.log(error);
            }
          );
      }
    });
  }

  showNetworkAlert() {
    let networkAlert = this.alertCtrl.create({
      title: "Нет соединения с интернетом!",
      message: "Пожалуйста, проверьте соединения к интернету.",
      buttons: [
        {
          text: "Отмена",
          handler: () => {
            this.isNetworkAlertActive = false;
          }
        },
        {
          text: "Настройки",
          handler: () => {
            this.isNetworkAlertActive = false;
            this.showSettings();
          }
        }
      ]
    });

    this.isNetworkAlertActive = true;
    networkAlert.present();
  }

  private showSettings() {
    this.diagnostic.switchToWifiSettings();
  }

  openPage(page) {
    this.nav.setRoot(page.component);
    this.activePage = page;
  }

  logout() {
    let tn = JSON.parse(localStorage.getItem("tn"));
    tn["token"] = "";
    this.authService.userIdForPush = "";
    localStorage.setItem("tn", JSON.stringify(tn));
    this.storageService.clearStorage();
    this.navCtrl.setRoot(LoginPage);
  }
}