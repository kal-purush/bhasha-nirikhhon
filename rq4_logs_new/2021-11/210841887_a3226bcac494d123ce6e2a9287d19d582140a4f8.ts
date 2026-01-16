import { Injectable } from '@angular/core';
import { StoreService } from './store.service';
import { Brand } from '@perun-web-apps/perun/openapi';

@Injectable({
  providedIn: 'root'
})
export class OtherApplicationsService {

  constructor(private storeService: StoreService) { }

  /**
   * Returns brand according to base domain or default brand
   *
   * @param brands array of brands from configuration
   * @param domain base domain of running app (Example: https://perun-dev.cz)
   */
  private static getBrandContainingDomain(brands: Brand[], domain: string): Brand {
    for (const brand of brands) {
      if (brand.newApps.admin === domain || brand.newApps.profile === domain || brand.newApps.pwdReset === domain) {
        return brand;
      }
    }
    return brands[0];
  }

  /**
   * This function returns url for application based on appType given in parameter.
   *
   * URL of other application differs according to base url of running application
   * and brands configuration. If base domain doesn't match any url in brands configuration,
   * then will be used default brand.
   * If new application url is not defined in brand configuration, then will be used old gui.
   *
   * @param appType type of requested app (admin | profile | pwdReset)
   * @param login login namespace for pwd reset app
   */
  getUrlForOtherApplication(appType: string, login?: string): string {
    const currentUrl = window.location.href;
    const splittedUrl = currentUrl.split("/");
    const domain = splittedUrl[0] + "//" + splittedUrl[2]; // protocol with domain

    const brand = OtherApplicationsService.getBrandContainingDomain(this.storeService.getAppsConfig().brands, domain);
    let url: string;

    if (!brand.newApps[appType]) {
      // url for new app of appType doesn't exist - set url to old gui
      url = brand.oldGuiDomain + "/fed";

      switch (appType) {
        case "admin":
          url += "/gui/"
          break;
        case "profile":
          url += "/profile/"
          break;
        case "pwdReset":
          url += "/pwd-reset/"
          break;
      }
    } else {
      url = brand.newApps[appType];
      if (appType === "pwdReset") {
        url += `?login-namespace=${login}`
      }
    }

    return url;
  }
}