import React from 'react';
var Firebase = require('firebase');
var ReactFire = require('reactfire');
var fbutil = require('firebase-util');
// var h = require('../helpers');
var $ = require('jquery');
var Mousetrap = require('mousetrap')
var Loader = require('react-loader');
// http://getbootstrap.com/components/#pagination
var Pagination = require('react-bootstrap').Pagination

import Video from './video'

var App = React.createClass({
  // mixins: [ReactFireMixin],
  getInitialState: function(){
    return {
      videos: [],
      pageSize: 10,
      page: 1,
      pageCount: 0
    };
  },
  componentWillMount: function(){
    var ref = new Firebase("https://confreaks.firebaseio.com/videos");
    // this.firebaseCursor = new Firebase.util.Scroll(ref, 'views');
    this.firebaseCursor = new Firebase.util.Paginate(ref, 'negative_views', {
      pageSize: this.state.pageSize
    });

    this.firebaseCursor.on("child_added", function(dataSnapshot) {
      // console.dir(dataSnapshot);
      this.state.videos.push(dataSnapshot.val());
      this.setState({
        videos: this.state.videos
      });
    }.bind(this));

    // NOTE: this function is misspelled. Change to getCountByDownloadingAllKeys
    // when https://github.com/firebase/firebase-util/pull/82 is merged
    this.firebaseCursor.page.getCountByDowloadingAllKeys(function(count){
      console.info("downloaded total count = ", count)
      this.gotoPage(this.state.page);
    }.bind(this))

    this.firebaseCursor.page.onPageCount(function(currentPageCount, couldHaveMore) {
      this.setState({
        pageCount: currentPageCount
      });
    }.bind(this));

    this.firebaseCursor.page.onPageChange(function(pageNumber){
      // ignore initial loading
      if(pageNumber !== 0){
        console.info("on page " + pageNumber);
        this.setState({
          page: pageNumber
        });
      }
    }.bind(this));
  },
  componentDidMount: function(){
    Mousetrap.bind('right', function() { this.nextPage() }.bind(this));
    Mousetrap.bind('left', function() { this.prevPage() }.bind(this));
  },
  nextPage: function(){
    this.setState({
      videos: []
    });
    this.firebaseCursor.page.next();
  },
  prevPage: function(){
    this.setState({
      videos: []
    });
    this.firebaseCursor.page.prev();
  },
  gotoPage: function(pageNumber){
    this.setState({
      videos: []
    });
    this.firebaseCursor.page.setPage(pageNumber);
  },
  // loadSampleVideos: function(){
  //   console.log("loading samples");
  //   this.setState({
  //     videos: require('./sample-videos')
  //   })
  // },
  handlePaginationSelect: function(event, selectedEvent){
    this.gotoPage(selectedEvent.eventKey);
  },
  paginator: function(){
    return <Pagination onSelect={this.handlePaginationSelect} activePage={this.state.page} items={this.state.pageCount} maxButtons={10} first={true} last={false} next={true} prev={true} />;
  },
  render: function(){
    return (
      <div className="">
        {this.paginator()}
        <Loader loaded={this.state.videos.length}>
          {this.state.videos.map(function(video){
            return <Video key={video.id} {...video} />
          })}
        </Loader>
        {this.paginator()}
      </div>
    )
  }
});

export default App;