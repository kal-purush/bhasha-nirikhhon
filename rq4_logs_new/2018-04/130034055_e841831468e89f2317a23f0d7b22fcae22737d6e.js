import React, { Component } from 'react';
import { Container, Item, Input, Icon } from 'native-base';

export default class SearchBar extends Component {

  firstSearch() {
    this.searchDirectory(this.itemsRef);
    console.log('inne i firstSearch');
  }

  searchDirectory(itemsRef) {
    const searchText = this.state.searchText.toString();

    if (searchText === '') {
      this.listenForItems(itemsRef);
    } else {
itemsRef.orderByChild('searchable').startAt(searchText).endAt(searchText).on('value', (snap) => {
        const items = [];
        snap.forEach((child) => {
          items.push({
            name: child.val().name,
            phone: child.val().phone,
          });
        });


        this.setState({
          dataSource: this.state.dataSource.cloneWithRows(items)
        });
      });
    }
  }


  render() {
    return (
      <Container searchBar rounded>
      <Item style={{ backgroundColor: '#d1d1d1', borderRadius: 15 }}>
      <Icon name="ios-search" style={{ paddingLeft: 5 }} />
      <Input
        returnKeyType='search'
        onChangeText={(text) => this.setState({ searchText: text })}
        onSubmitEditing={() => this.firstSearch()}
        placeholder="Search"
      />
      </Item>
      {  // <Button transparent>
        //   <Text>Search</Text>
        // </Button>
      }
      </Container>
    );
 }
}