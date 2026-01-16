
declare module 'project-packages-filters' {
  function vendorPackages(packageJsonLocation?: string, literals?: string[], regex?: string[], useDefaults?: boolean): string[];
  function helperPackages(packageJsonLocation?: string, literals?: string[], regex?: string[], useDefaults?: boolean): string[];
}