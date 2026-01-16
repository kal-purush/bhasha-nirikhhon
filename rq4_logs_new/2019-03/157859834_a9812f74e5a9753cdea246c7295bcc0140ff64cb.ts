import { connect } from "react-redux";

import Home from "./Home";
import Research from "./Research";
import Portfolio from "./Portfolio";

import { 
  Area, 
  AreaVals, 
  AreaFuncs, 
  AreaProps, 
} from "./Area";

import { 
  NavBar, 
  NavBarVals, 
  NavBarFuncs, 
  NavBarProps, 
} from "../components/Nav";

import { setTitle } from "../store/about/about.actions";

import { selectSubNavLabels } from "../store/nav/nav.selectors";

import { StoreType } from "../types";

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

const HomeContainer = connect<AreaVals, AreaFuncs, {}>
(
  (state: StoreType, props: AreaProps) => ({
    area: '',
    title: 'Howard Melnyczuk',
    component: Home,
  }),
  (dispatch: Function, props: AreaProps) => ({
    updateTitle: (title: string) => dispatch(setTitle(title))
  })
)(Area);

const PortfolioContainer = connect<AreaVals, AreaFuncs, {}>
(
  (state: StoreType, props: AreaProps) => ({
    area: 'portfolio',
    title: 'Portfolio',
    component: Portfolio,
  }),
  (dispatch: Function, props: AreaProps) => ({
    updateTitle: (title: string) => dispatch(setTitle(title))
  })
)(Area);

const ResearchContainer = connect<AreaVals, AreaFuncs, {}>
(
  (state: StoreType, props: AreaProps) => ({
    area: 'research',
    title: 'Research',
    component: Research,
  }),
  (dispatch: Function, props: AreaProps) => ({
    updateTitle: (title: string) => dispatch(setTitle(title))
  })
)(Area);

export {
  HomeContainer,
  PortfolioContainer,
  ResearchContainer,
  SubNavContainer,
}