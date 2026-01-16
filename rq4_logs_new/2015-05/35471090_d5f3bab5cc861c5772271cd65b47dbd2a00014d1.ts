class ItemsComponent<T> extends Component {
    // Items Source
    public itemsSource: Observable<ObservableArray<T>>;

    // Callbacks
    public itemsSourceChanged: (args: ValueChangedEvent<ObservableArray<T>>) => void;
    public itemAdded: (args: ObservableArrayEventArgs<T>) => void;
    public itemRemoved: (args: ObservableArrayEventArgs<T>) => void;

    constructor() {
        // Set up callbacks
        this.itemAdded = (args) => {
            this.processItemAdded(args.item, args.position);
        }
        this.itemRemoved = (args) => {
            this.processItemRemoved(args.item, args.position);
        }
        this.itemsSourceChanged = (args) => {
            args.oldValue.itemAdded.unSubscribe(this.itemAdded);
            args.oldValue.itemRemoved.unSubscribe(this.itemRemoved);
            args.newValue.itemAdded.subscribe(this.itemAdded);
            args.newValue.itemRemoved.subscribe(this.itemRemoved);
        }

        // Set up items source
        this.itemsSource = new Observable<ObservableArray<T>>();
        this.itemsSource.onValueChanged.subscribe(this.itemsSourceChanged);
        this.itemsSource.value = new ObservableArray<T>();
        
        super("Items");
    }

    private processItemAdded(item: T, position: number) {

    }

    private processItemRemoved(item: T, position: number) {

    }
}