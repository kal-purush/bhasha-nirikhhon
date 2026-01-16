import collectUpdates from '@lerna/collect-updates';
import PackageGraph from '@lerna/package-graph';
import Project from '@lerna/project';

type LernaPackage = {
  name: string;
};

type Updated = {
  pkg: LernaPackage;
};

/**
 * Get the list of changed packages as a string
 * @param cwd Current working directory
 */
export const changedPackages = async (cwd: string) => {
  const project = new Project(cwd);
  const packageGraph = new PackageGraph(await project.getPackages());
  const execOptions = { cwd };
  const options = {};
  const updates: LernaPackage[] = (<Updated[]>collectUpdates(packageGraph.rawPackageList, packageGraph, execOptions, options)).map(
    (node) => node.pkg,
  );

  return updates.length ? `{${updates.map((pkg) => pkg.name).join(',')}}` : '';
};