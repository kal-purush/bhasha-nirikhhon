// tslint:disable
declare module ReactHotLoader {
  import React = __React;

  interface AppContainerProps extends React.HTMLAttributes {

  }

  export class AppContainer extends React.Component<AppContainerProps, any> {}
}

declare module 'react-hot-loader' {
  export = ReactHotLoader;
}