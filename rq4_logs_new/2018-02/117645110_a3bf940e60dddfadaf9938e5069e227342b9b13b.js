import React, { Component } from 'react';
import {
    Text,
    Image,
    StyleSheet,
    FlatList,
    View
} from 'react-native';

import Log from 'react-native-log';

export default class ShopHeaderView extends Component {

    constructor(props) {
        super(props);
        this.state = {
            dataSource:this.props.array,
        };
    }

    _flatListItem = ({item}) => {
        Log.i(item.image);
        return (
            <View style={{width:ScreenWidth / 4, height: 80, justifyContent:'center', alignItems:'center'}}>
                <Image style={{width:ScreenWidth / 4 / 2, height: ScreenWidth / 4 / 2}} source={{uri:item.image}}/>
                <Text style={{marginTop:10}}>{item.title}</Text>
            </View>
        )
    };

    render() {
        return(
            <View style={styles.container}>
                <FlatList style={styles.container}
                          data={this.state.dataSource}
                          renderItem={this._flatListItem}
                          keyExtractor={(item, index) => 'index' + item + index}
                          showsHorizontalScrollIndicator={false}
                          showsVerticalScrollIndicator={false}
                          numColumns={4}
                />
            </View>
        )
    }
}

const styles = StyleSheet.create({
    container: {
        flex:1,
    },
});