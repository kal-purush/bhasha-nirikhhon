import React, { Component } from 'react'

import RaisedButton from 'material-ui/RaisedButton'
import FlatButton from 'material-ui/FlatButton'
import Divider from 'material-ui/Divider'
import {Card, CardText, CardTitle } from 'material-ui/Card'
import CircularProgress from 'material-ui/CircularProgress';
import FloatingActionButton from 'material-ui/FloatingActionButton'
import FileFileDownload from 'material-ui/svg-icons/file/file-download'
import ActionSettings from 'material-ui/svg-icons/action/settings'
import ImageEdit from 'material-ui/svg-icons/image/edit'
import { Step, Stepper, StepButton } from 'material-ui/Stepper'

class PageButtons extends Component {
  constructor(props) {
    super(props)
    this.backPage = this.backPage.bind(this)
    this.nextPage = this.nextPage.bind(this)
    this.changePageObj = {}
    props.pages.forEach(page => {
      this.changePageObj[page] = this.changePage.bind(this, page)
    })
  }

  changePage(page) {
    const { onChangePage } = this.props
    onChangePage(page)
  }

  movePage(num) {
    const { page, pages } = this.props
    let next = pages[pages.length - 1]
    for (let i = pages.length - 1; i >= 0; i --) {
      if (page == pages[i]) {
        next = pages[(i + pages.length + num) % pages.length]
        break
      }
    }
    this.changePage(next)
  }

  backPage() {
    this.movePage(-1)
  }

  nextPage(page) {
    this.movePage(+1)
  }

  render() {
    const { page, getPageName, pages } = this.props
    const index = pages.indexOf(page)
    const buttons = []
    for (let i = 0; i < pages.length; i ++) {
      buttons[i] = (
        <Step key={i}>
          <StepButton
            onClick={this.changePageObj[pages[i]]}
          >{getPageName(pages[i])}</StepButton>
        </Step>
      )
    }
    return (
      <span>
        <Stepper activeStep={index} linear={false}>
          {buttons}
        </Stepper>
        <FlatButton onClick={this.backPage} style={{ marginLeft: '3%' }} disabled={index === 0}>戻る</FlatButton>
        <RaisedButton onClick={this.nextPage} primary={true} style={{ marginLeft: '3%' }}>次へ</RaisedButton>
      </span>
    )
  }
}

const HostPage = ({
  children,
  page,
  getPageName,
  pages,
  changePage,
  openSettingDialog = () => {},
  openEditDialog = () => {},
  downloadFile = () => {},
  loading = false,
  settingButton = false,
  editButton = false,
  downloadButton = false,
}) => {
  if (loading) {
    return (
      <Card style={{padding: '20px'}}>
        <CardTitle title="接続中" style={{padding: '0px', marginTop: '7px', marginBottom: '14px'}}/>
        <CardText style={{padding: '0px', margin: '0px'}}>
          <div style={{textAlign: 'center'}}>
            <CircularProgress style={{margin: '0px', padding: '0px' }} />
          </div>
          <p style={{margin: '0px', padding: '0px'}}>サーバーに接続しています。<br/>このまましばらくお待ちください。</p>
        </CardText>
      </Card>
    )
  } else {
    return (
      <div>
        <PageButtons
          page={page}
          getPageName={getPageName}
          pages={pages}
          onChangePage={changePage}
        />
        <Divider
          style={{
            marginTop: "3%",
            marginBottom: "3%",
          }}
        />
        <div>
          {children}
        </div>
        <Divider
          style={{
            marginTop: "3%",
            marginBottom: "3%",
          }}
        />
        <FloatingActionButton
          onClick={openSettingDialog}
          disabled={!settingButton}
        >
          <ActionSettings />
        </FloatingActionButton>
        <FloatingActionButton
          onClick={openEditDialog}
          style={{marginLeft: "2%"}}
          disabled={!editButton}
        >
          <ImageEdit />
        </FloatingActionButton>
        <FloatingActionButton
          style={{marginLeft: "2%"}}
          disabled={!downloadButton}
          onClick={downloadFile}
        >
          <FileFileDownload />
        </FloatingActionButton>
      </div>
    )
  }
}

HostPage.propTypes = {
  children: React.PropTypes.node.isRequired,
  page: React.PropTypes.string.isRequired,
  getPageName: React.PropTypes.func.isRequired,
  pages: React.PropTypes.arrayOf(React.PropTypes.string).isRequired,
  changePage: React.PropTypes.func.isRequired,
  openEditDialog: React.PropTypes.func,
  openSettingDialog: React.PropTypes.func,
  downloadFile: React.PropTypes.func,
  loading: React.PropTypes.bool,
  settingButton: React.PropTypes.bool,
  editButton: React.PropTypes.bool,
  downloadButton: React.PropTypes.bool,
}

export default HostPage