export class Skill {
  readonly id: string;

  readonly name: string;

  readonly description: string;

  readonly officialLink: string;

  readonly iconUrl: string;

  readonly createdAt: Date;

  constructor(args: { id: string; name: string; description: string; officialLink: string; iconUrl: string; createdAt: Date }) {
    this.id = args.id;
    this.name = args.name;
    this.description = args.description;
    this.officialLink = args.officialLink;
    this.iconUrl = args.iconUrl;
    this.createdAt = args.createdAt;
  }
}