import { RolesEnum } from '../context/CalculatorContext';

export function getPlatform(title: string) {
  switch (title) {
    case 'Apple iOS':
      return { title: 'Apple iOS', roles: [RolesEnum.MD] };
    case 'Android':
      return { title: 'Android', roles: [RolesEnum.MD] };
    case 'Web':
      return { title: 'Web', roles: [RolesEnum.FE] };
    case 'Multi-Platform':
      return { title: 'Multi-Platform', roles: [RolesEnum.BE, RolesEnum.FE] };
    default:
      return { title: '', roles: [] };
  }
}

export function getPlatformSize(title: string) {
  switch (title) {
    case 'Small':
      return 'Small';
    case 'Medium':
      return 'Medium';
    case 'Large':
      return 'Large';
    default:
      '';
  }
}

export function getDesigner(title: string) {
  switch (title) {
    case 'Yes':
      return 'Yes';
    case 'No':
      return 'No';
    default:
      '';
  }
}