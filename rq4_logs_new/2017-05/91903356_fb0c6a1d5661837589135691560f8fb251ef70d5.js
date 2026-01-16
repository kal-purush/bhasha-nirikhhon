export default function MaterialColorConfig($mdThemingProvider) {
  "ngInject";

  var BlueShadeMap = $mdThemingProvider.extendPalette('indigo', {
    '500': '#4B7796',
    'contrastDefaultColor': 'dark'
  });

  $mdThemingProvider.definePalette('BlueShade', BlueShadeMap);

  $mdThemingProvider.theme('default')
  .primaryPalette('BlueShade');

}