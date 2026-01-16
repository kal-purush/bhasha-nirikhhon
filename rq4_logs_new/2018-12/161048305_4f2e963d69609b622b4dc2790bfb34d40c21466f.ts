import { FormAttribute } from "./base/forms/interfaces/formModel";

export class Structure
{
  public hasBeenSorted: boolean = false;

  private structure: StructureType = null;

  public constructor(structure: StructureType)
  {
    this.structure = structure;

    if (!this.structure.config) {
      this.structure.config = {};
    }
  }

  public getItems(): { [name: string]: FormAttribute }
  {
    return this.structure.items;
  }

  public getName(): string
  {
    return this.structure.name;
  }

  public getParentName(): string
  {
    return this.structure.parent;
  }

  public getLabel(): string
  {
    return this.structure.label;
  }

  public getConfig(): StructureConfig
  {
    return this.structure.config;
  }

  public getAddLineText(): string 
  {
    return this.getConfig().addLineText;
  }

  public canEdit(): boolean
  {
    return this.getConfig().canEdit;
  }

  public setCanEdit(canEdit: boolean): void
  {
    this.getConfig().canEdit = canEdit;
  }

  public canAdd(): boolean
  {
    return this.getConfig().canAdd;
  }

  public setCanAdd(canAdd: boolean): void
  {
    this.getConfig().canAdd = canAdd;
  }

  public useModalEdit(): boolean
  {
    return this.getConfig().useModalEdit;
  }

  public isSortable(): boolean
  {
    return this.getConfig().isSortable;
  }
}

export interface StructureType
{
  /**
   * Name of tree branch
   */
  name: string;

  /**
   * Parent tree branch
   */
  parent: string | null;

  /**
   * Visible name of tree branch
   */
  label?: string;

  /**
   * Other level options
   */
  config?: StructureConfig;

  /**
   * Structure of items displayed
   */
  items?: { [name: string]: FormAttribute };
}

export type StructureConfig = {
  /**
   * Is this level editable
   */
  canEdit?: boolean;

  /**
   * Is it possible to add lines on this level
   */
  canAdd?: boolean;

  /**
   * Show modal for line editing
   */
  useModalEdit?: boolean;

  /**
   * Text of add line
   */
  addLineText?: string;

  /**
   * Can lines on this level be manually sorted
   */
  isSortable?: boolean;
}