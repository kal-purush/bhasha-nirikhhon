import { Component, createElement, ReactElement, ReactNode } from 'react';

type ErrorBoundaryProps = {
  children: ReactNode;
  fallback: () => ReactElement;
};

type ErrorBoundaryState = {
  hasError: boolean;
  error?: any;
  info?: any;
};

class ErrorBoundary extends Component<ErrorBoundaryProps, ErrorBoundaryState> {
  constructor(props: any) {
    super(props);
    this.state = { hasError: false };
  }

  static getDerivedStateFromError() {
    return { hasError: true };
  }

  componentDidCatch(error: any, info: any) {
    this.setState({ error, info });
  }

  render() {
    const { children, fallback } = this.props;

    if (this.state.hasError) {
      return createElement(fallback);
    }

    return children;
  }
}

export default ErrorBoundary;