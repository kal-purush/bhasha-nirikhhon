import { connect } from 'react-redux';

import {
  NavBarVals,
  NavBarFuncs,
  NavBar,
  NavBarProps
} from '../../components/Nav';

import {
  Works, WorksVals, WorksFuncs, WorksProps,
} from './Works';

import {
  selectSubNavLabels,
} from '../../store/nav/nav.selectors';

import {
  StoreType,
} from '../../types';
import { worksSelectors } from '../../store/works/works.selectors';

const SubNavContainer = connect<NavBarVals, NavBarFuncs, {}>
  (
    (state: StoreType, props: NavBarProps) => ({
      className: '',
      buttonClassName: '',
      labels: selectSubNavLabels(state),
      title: 'portfolio',
    }),
    (dispatch: Function, props: NavBarProps) => ({})
  )(NavBar);


const WorksContainer = connect<WorksVals, WorksFuncs, {}>
  (
    (state: StoreType, props: WorksProps) => ({
      works: worksSelectors.selectWorks(state),
      filter: props.match.params.filter,
    }),
    (dispatch: Function, props: WorksProps) => ({}),
  )(Works);

export {
  SubNavContainer,
  WorksContainer,
}