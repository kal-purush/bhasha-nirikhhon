import React , {PropTypes} from 'react';
import {Link} from 'react-router';
import {Table, TableBody, TableHeader, TableHeaderColumn, TableRow, TableRowColumn} from 'material-ui/Table';
import FloatingActionButton from 'material-ui/FloatingActionButton';
import ContentCreate from 'material-ui/svg-icons/content/create';
import ContentAdd from 'material-ui/svg-icons/content/add';
import {pink500, grey200, grey500, red500 ,greenA200} from 'material-ui/styles/colors';
import PageBase from '../components/PageBase';
import SvgIcon from 'material-ui/SvgIcon';
import Data from '../data';
import IconButton from 'material-ui/IconButton';
import Popover from 'material-ui/Popover';
import Menu from 'material-ui/Menu';
import MenuItem from 'material-ui/MenuItem';
import Dialog from 'material-ui/Dialog';
import FlatButton from 'material-ui/FlatButton';
import TextField from 'material-ui/TextField';
import { connect } from 'react-redux'
import { loadOpenodes, callAddOpenode} from '../actions'
import RaisedButton from 'material-ui/RaisedButton';

const DeleteIcon = (props) => (
    <SvgIcon {...props}>
        <path d="M6 19c0 1.1.9 2 2 2h8c1.1 0 2-.9 2-2V7H6v12zM19 4h-3.5l-1-1h-5l-1 1H5v2h14V4z"/>
        <path d="M0 0h24v24H0z" fill="none"/>
    </SvgIcon>
);
const DetailIcon = (props) => (
  <SvgIcon {...props}>
    <path d="M0 0h24v24H0z" fill="none"/>
    <path d="M3 4l9 16 9-16H3zm3.38 2h11.25L12 16 6.38 6z"/>
  </SvgIcon>
)

const RefreshIcon = (props) => (
  <SvgIcon {...props}>
    <path d="M17.65 6.35C16.2 4.9 14.21 4 12 4c-4.42 0-7.99 3.58-7.99 8s3.57 8 7.99 8c3.73 0 6.84-2.55 7.73-6h-2.08c-.82 2.33-3.04 4-5.65 4-3.31 0-6-2.69-6-6s2.69-6 6-6c1.66 0 3.14.69 4.22 1.78L13 11h7V4l-2.35 2.35z"/>
    <path d="M0 0h24v24H0z" fill="none"/>
  </SvgIcon>
);

const styles = {
    radioButton: {
     marginBottom: 16,
    },
    floatingActionButton: {
      margin: 0,
      top: 'auto',
      right: 20,
      bottom: 20,
      left: 'auto',
      position: 'fixed',
    },
    editButton: {
      fill: grey500
    },
    columns: {
      id: {
        width: '10%'
      },
      url: {
        width: '15%'
      },
      ip: {
        width: '10%'
      },
      actions: {
        width: '15%'
      },
      country: {
        width: '10%'
      },
    },
    btnLoad : {
        marginTop : 10
    },
    input : {
      marginLeft : 10,
    }
  };


class OpenodeManagementPage extends React.Component  {
    
    constructor(props) {
        super(props);
    
        this.state = {
          open: false,
          openAddOpenode : false,
          siteUrl : '',
          page : 1,
          per_page : 20 ,
          loadding : false,
          openode : {}
        };
    }
    componentDidMount(){
     this.props.loadOpenodes({page : 1, per_page : 20});
    
    }
    componentWillReceiveProps(newProps) {
      //console.log(newProps)
      this.setState({loadding : false})
    }
    
    handleTouchTap = (event) => {
         // This prevents ghost click.
        event.preventDefault();

        this.setState({
            openAddOpenode: true
        });
    };

    handleRequestClose = () => {
        this.setState({
            open: false,
            openAddOpenode : false,
        });
    };
    
    
    handleOpenAddOpenode = (type) => {
        this.handleRequestClose();
        this.setState({openAddOpenode: true});
    };
    
    handleCloseAddOpenode = () => {
        this.setState({openAddOpenode: false});
    };
    
    handleAddOpenode = () => {
      if(this.state.siteUrl.length > 0 ){
        this.props.callAddOpenode({siteUrl : this.state.siteUrl})
        this.setState({openAddOpenode: false});
      }
    };
    handleOnChangeSiteUrl = (event) =>{
      this.setState({
        siteUrl: event.target.value
      });
    }
    
    handleLoadGroups = () => {
      this.props.loadGroup({page : this.state.page, per_page : this.state.per_page});
      this.setState({loadding : true})
    }
    
    handleOnChangePageNumber = (event) =>{
      this.setState({
        page: event.target.value,
      });
    }
    
    handleOnChangePerPage = (event) =>{
      this.setState({
        per_page: event.target.value,
      });
    }
    handleDeleteOpenode = (id) => {
      //this.props.deleteOpenode({id: id })
    }
 

  

    render(){
      const {openodes, meta} = this.props;
      const {openode } = this.state;
      const actions = [
            <FlatButton
              label="Ok"
              primary={true}
              keyboardFocused={true}
              onClick={this.handleAddOpenode.bind(this)}
            />
        ];  
    return (
        <PageBase title="Openode Management Page"
                  navigation="Application / Openode Management Page">
                  
        <div>
            <RaisedButton label="Load" style={styles.btnLoad} primary={true} onClick = {this.handleLoadGroups} disabled = {this.state.loadding}/>
            <TextField
                  style ={styles.input}
                  hintText={"Page Number"}
                  floatingLabelText={"Page Number"}
                  onChange = {this.handleOnChangePageNumber}
                  />
             <TextField
                  style ={styles.input}
                  hintText={"Row per Page"}
                  floatingLabelText={"Row per Page"}
                  onChange = {this.handleOnChangePerPage}
                  />      
            {meta ?  "    Total: "  + meta.total : null}
            <Dialog
                title="Add Openode"
                actions={actions}
                modal={false}
                open={this.state.openAddOpenode}
                onRequestClose={this.handleCloseAddOpenode}
                autoScrollBodyContent={true}
            >
                <TextField
                  hintText={"sharefacebook001.fr.openode.io"}
                  floatingLabelText={"Site Url:"}
                  fullWidth={true}
                  onChange={this.handleOnChangeSiteUrl}
                />
            </Dialog>
            
         
            <FloatingActionButton onClick={this.handleTouchTap} style={styles.floatingActionButton} backgroundColor={pink500}>
                <ContentAdd />
            </FloatingActionButton>
    
            <Table selectable={false}>
              <TableHeader adjustForCheckbox={false} displaySelectAll={false}>
                <TableRow>
                  <TableHeaderColumn style={styles.columns.id}>ID</TableHeaderColumn>
                  <TableHeaderColumn style={styles.columns.url}>URL</TableHeaderColumn>
                  <TableHeaderColumn style={styles.columns.ip}>Ip</TableHeaderColumn>
                  <TableHeaderColumn style={styles.columns.actions}>Actions</TableHeaderColumn>
                </TableRow>
              </TableHeader>
              <TableBody displayRowCheckbox={false}>
                {openodes && openodes.length > 0 ? openodes.map(item =>
                  <TableRow key={item.id}>
                    <TableRowColumn style={styles.columns.id}>{item.id}</TableRowColumn>
                    <TableRowColumn style={styles.columns.url}>{item.siteUrl}</TableRowColumn>
                    <TableRowColumn style={styles.columns.ip}>
                        {item.ip}
                    </TableRowColumn>
                    <TableRowColumn style={styles.columns.actions}>
                        <IconButton onClick={() => this.handleDeleteOpenode(item.id)}>
                          <DeleteIcon  color={red500} />
                        </IconButton>
                       
                    </TableRowColumn>
                  </TableRow>
                ) : null}
              </TableBody>
            </Table>    
          </div>
        </PageBase>
      );
    }
};

OpenodeManagementPage.propTypes = {
    
}

export default connect(
  state => ({openodes : state.entities.openodes, meta : state.entities.meta}),
  {loadOpenodes, callAddOpenode}
)(OpenodeManagementPage)