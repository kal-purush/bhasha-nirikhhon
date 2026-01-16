import React from "react";
import {FlatList, StatusBar} from "react-native";
import {Screen} from "../elements/box/screen/Screen";
import {ItemModelAdaptor, ItemReport} from "./ItemReport";
import moment from "moment/moment";
import {
    ReportAction,
    ReportEpicFollowUpAction
} from "../service/ReportEpicActions";
import {store} from "../utils/store";
import {Icon} from "react-native-elements";
import {EditText} from "../elements/components/EditText";
import {HBox} from "../elements/box/HBox";

export default class SearchMyReports extends React.Component {

    static navigationOptions = {
        header: null,
    };

    constructor(props) {
        super(props)
        this.state = {
            token: store.getState().systemReducer.token,
            results: store.getState().reportsReducer.searchResults,
            term: ""
        }
    }

    __unsubscribeReportsObserver = store.subscribe(() =>
        this.setState({results: store.getState().reportsReducer.searchResults}))


    componentWillUnmount = () => this.__unsubscribeReportsObserver()

    __getNextPageOfReports = () => {
        const token = store.getState().systemReducer.token
        if (!this.state.results.isLast && isFinite(this.state.results.page)) {
            store.dispatch(new ReportAction(token)
                .search(this.state.term, this.state.results.page + 1))
        }
    }

    __adaptToItemView = (data) =>
        new ItemModelAdaptor(data.id, data.title, data.text,
            moment(data.date).fromNow(), data.location, data.type,
            data.photos, data.spam, data.solved)

    __showNewReportsToUserInList = (items) =>
        <FlatList
            data={items}
            onEndReached={() => this.__getNextPageOfReports()}
            keyExtractor={(item, id) => id}
            renderItem={({item}) =>
                <ItemReport {...this.props}
                            item={this.__adaptToItemView(item)}/>}/>

    render = () =>
        <Screen backgroundColor={'white'}>
            <StatusBar
                backgroundColor="transparent"
                barStyle="dark-content"/>
            <HBox style={{padding: 20, marginTop: 22}}
                  flex={0} height={70} alignItems={'center'}>
                <Icon
                    containerStyle={{width: 30, height: 30}}
                    onPress={() => this.props.navigation.goBack()}
                    name={'close'} color={'black'}/>
                <EditText
                    fontSize={16}
                    clearTextOnFocus={true}
                    text={'Search for reports ...'}
                    onEndEditing={() => this.state.term !== '' ? store.dispatch(
                        new ReportAction(this.state.token).search(this.state.term, 0)) : ''}
                    onChangeText={async (term) => {
                        term === '' ? store.dispatch(
                            ReportEpicFollowUpAction.clearSearch()) : ''
                        await this.setState({term: term})
                    }}/>
                <Icon
                    containerStyle={{width: 30, height: 30}}
                    onPress={() => store.dispatch(
                        new ReportAction(this.state.token)
                            .search(this.state.term, 0))}
                    name={'search'} color={'black'}/>
            </HBox>
            {this.__showNewReportsToUserInList(this.state.results.reports)}
        </Screen>
}
