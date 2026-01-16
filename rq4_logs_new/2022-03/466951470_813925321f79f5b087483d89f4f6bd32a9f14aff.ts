import {
  Application,
  Context,
  Converter,
  ReflectionCategory,
} from 'typedoc';

class Plugin {
  private app: Application;

  private categoryPatterns: RegExp[] = [];

  constructor(app: Application) {
    this.app = app;
  }

  initialize() {
    this.app.converter.on(Converter.EVENT_BEGIN, this.onBegin, this);
    this.app.converter.on(Converter.EVENT_RESOLVE_END, this.onResolveEnd, this);
  }

  onBegin() {
    this.categoryPatterns = (this.app.options.getValue('categoryPatterns') as string[])
      .map((v) => new RegExp(v));
  }

  extract(name: string): [string | undefined, string] {
    for (let i = 0; i < this.categoryPatterns.length; i += 1) {
      const match = this.categoryPatterns[i].exec(name);
      if (match && match.groups?.category && match.groups?.module) {
        return [match.groups.category, match.groups.module];
      }
    }

    return [undefined, name];
  }

  onResolveEnd(context: Context) {
    if (!context.project.children) return;

    const categories: Record<string, ReflectionCategory> = {};

    for (let i = 0; i < context.project.children.length; i += 1) {
      const child = context.project.children[i];
      const originalModuleName = child.name;
      const [categoryName, moduleName] = this.extract(originalModuleName);

      if (categoryName) {
        child.name = moduleName;

        categories[categoryName] ||= new ReflectionCategory(categoryName);
        categories[categoryName].children.push(child);

        this.app.logger.verbose(`Module ${originalModuleName} => [${categoryName}] ${moduleName}`);
      }
    }

    context.project.categories = Object.values(categories);
  }
}

export default Plugin;