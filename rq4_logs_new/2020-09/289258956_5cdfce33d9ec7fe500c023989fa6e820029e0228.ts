type WatchListComponentProps = {
  /** A list of symbols to be watch like `["BTC", "XRP", "XTZ"]` */
  symbols?: string[];
  /** Will be invoked when user chanes the order of symbols in watch list */
  onOrderChanged?: (symbols: string[]) => void;
};

type PriceTickers = { [k: string]: number };

type WatchListContext = {
  /** Shares symbol prices */
  priceTickers?: priceTickers;
  setPriceTickers?: React.Dispatch<React.SetStateAction<PriceTickers | undefined>>;
};

type WatchListItemComponentProps = {
  symbol?: string;
};