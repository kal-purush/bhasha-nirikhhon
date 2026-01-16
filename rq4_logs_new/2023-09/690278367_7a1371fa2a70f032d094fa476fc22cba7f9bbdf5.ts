import { randomUUID } from 'node:crypto';

export interface ProcedureProjectProps {
  createdAt?: Date | null;
  updatedAt?: Date | null;
  name: string;
  machineName: string;
  description: string;
}

export class ProcedureProject {
  private props: ProcedureProjectProps;
  private _id: string;

  constructor(props: ProcedureProjectProps) {
    // this._id = !id ? randomUUID() : id;
    this.props = props;

    if (props.machineName) {
      this.props.machineName = props.machineName
        .toLowerCase()
        .split(' ')
        .join('-')
        .replace(/[^a-z0-9-]/g, '');
    }
  }

  public get id(): string {
    return this._id;
  }

  public set createdAt(createdAt: Date) {
    this.props.createdAt = createdAt;
  }
  public get createdAt(): Date | null | undefined {
    return this.props.createdAt;
  }

  public set updateAt(updateAt: Date) {
    this.props.updatedAt = updateAt;
  }
  public get updateAt(): Date | null | undefined {
    return this.props.createdAt;
  }
  public set name(name: string) {
    this.props.name = name;
  }
  public get name(): string {
    return this.props.name;
  }

  public set machineName(machineName: string) {
    this.props.machineName = machineName;
  }
  public get machineName(): string {
    return this.props.machineName;
  }

  public set description(description: string) {
    this.props.description = description;
  }
  public get description(): string {
    return this.props.description;
  }
}