import {BreadcrumbsService} from "./breadcrumbs/breadcrumbs.service";
import {FooterController} from "./footer/footer.controller";
import TriLoader from "./loader/loader.directive";
import LoaderService from "./loader/loader-service";
import {triMenuDirective} from "./menu/menu.directive";
import {menuProvider} from "./menu/menu.provider";
import {triMenuItemDirective} from "./menu/menu-item.directive";
import NotificationsPanelController from "./notifications-panel/notifications-panel.controller";
import tableImage from "./table/table-cell-image.filter";
import triTable from "./table/table.directive";
import widget from "./widget/widget.directive";
import DefaultToolbarController from "./toolbars/toolbar.controller";
import TriWizard from "./wizard/wizard.directive";
import startFrom from "./table/table-start-from.filter";
import WizardFormProgress from "./wizard/wizard-form.directive";


export default angular
  .module('uiCore.components', [])
  .factory('uiCoreBreadcrumbsService', BreadcrumbsService)
  .controller('FooterController', FooterController)
  .directive('uiCoreLoader', TriLoader)
  .factory('uiCoreLoaderService', LoaderService)
  .directive('uiCoreMenu', triMenuDirective)
  .provider('uiCoreMenu', menuProvider)
  .directive('uiCoreMenuItem', triMenuItemDirective)
  .controller('NotificationsPanelController', NotificationsPanelController)
  .directive('triTable', triTable)
  .filter('tableImage', tableImage)
  .filter('startFrom', startFrom)
  .controller('DefaultToolbarController', DefaultToolbarController)
  .directive('uiCoreWidget', widget)
  .directive('uiCoreWizard', TriWizard)
  .directive('uiCoreWizardForm', WizardFormProgress);
