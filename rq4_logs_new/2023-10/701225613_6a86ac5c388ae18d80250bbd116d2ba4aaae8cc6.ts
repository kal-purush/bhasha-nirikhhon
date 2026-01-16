export class ResourceNamingConvention {
  static readonly AVAILABLE_ENV_LIST = ["Dev", "Stg", "Prod", "Management"];
  readonly systemEnv: string;
  readonly systemService: string;
  constructor(systemName: string, systemEnv: string) {
    this.validate(systemName);
    this.systemEnv = systemEnv;
    this.systemService = systemName;
  }

  private validate(systemName: string): void {
    if (!ResourceNamingConvention.AVAILABLE_ENV_LIST.includes(systemName)) {
      throw new Error(
        `The name "${systemName}" must be included in the list ${ResourceNamingConvention.AVAILABLE_ENV_LIST}`,
      );
    }
  }

  generate(name: string): string {
    return `${this.systemService}-${this.systemEnv}-${name}`;
  }

  generateWithResource(resource: string, name: string): string {
    return `${this.systemService}-${this.systemEnv}-${resource}-${name}`;
  }
}