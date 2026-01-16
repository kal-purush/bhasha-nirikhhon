import { CellProps, TableInstance, UseGlobalFiltersInstanceProps } from 'react-table';

export type OverviewTableComponentProps = {
  /** The vaults to be presented on table */
  vaultsData: Vault[];
  /** Will be invoked when the user wants to add a trade, perhaps by clicking the `AddTrade` button. */
  onAddTrade: () => void;
  /** Will be invoked when a vault's pin button toggled. */
  onVaultPinToggled?: (vault: Vault) => void;
  className?: string;
};

export type OverviewTableInstance = TableInstance<Vault> & Partial<UseGlobalFiltersInstanceProps<Vault>>;

export type OverviewTableActionFn = (vault: Vault) => void;
export type OverviewTableVaultActions = {
  onPinClicked?: OverviewTableActionFn;
};

export type OverviewTableCellProps = CellProps<Vault> & OverviewTableVaultActions;