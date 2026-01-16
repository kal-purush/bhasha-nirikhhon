import { alias, tag, Events, Store, Tag } from '@storefront/core';

@alias('collections')
@tag('gb-collections', require('./index.html'))
class Collections {

  props: Collections.Props = {
    labels: {}
  };
  state: Collections.State = {
    collections: [],
    onSelect: (index) => this.flux.switchCollection(this.state.collections[index].value)
  };

  init() {
    this.updateCollections();
    this.flux.on(Events.COLLECTION_UPDATED, this.updateCollectionTotal);
    this.flux.on(Events.SELECTED_COLLECTION_UPDATED, this.updateCollections);
    this.services.collections.register(this);
  }

  updateCollections = () =>
    this.set({
      collections: this.selectCollections(this.flux.store.getState())
    })

  updateCollectionTotal = ({ name, total }: Store.Collection) => {
    const index = this.flux.store.getState()
      .data.collections.allIds.indexOf(name);
    const collections = this.state.collections.slice();

    collections.splice(index, 1, { ...this.state.collections[index], total });
    this.set({ collections });
  }

  selectCollections({ data: { collections } }: Store.State) {
    return collections.allIds.map((collection) => ({
      value: collection,
      label: this.props.labels[collection] || collection,
      selected: collections.selected === collection,
      total: collections.byId[collection].total
    }));
  }
}

interface Collections extends Tag<Collections.Props, Collections.State> { }
namespace Collections {
  export interface Props {
    labels: { [collection: string]: string };
  }

  export interface State {
    collections: Option[];
    onSelect(index: number): void;
  }

  export interface Option {
    value: string;
    label: string;
    selected?: boolean;
  }
}

export default Collections;