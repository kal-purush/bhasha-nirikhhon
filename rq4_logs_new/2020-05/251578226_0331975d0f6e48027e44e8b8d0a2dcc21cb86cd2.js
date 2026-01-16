const defaultState = {
  mediaList: [],
  medias: {}
}

const media = (state = defaultState, action) => {
  switch (action.type) {
    case 'GET_MEDIA_FROM_FOLDER_SUCCESS': {
      var mediaList = []
      var medias = {}
      action.medias.forEach(media => { 
        mediaList.push(media.mid[0])
        medias[media.mid[0]] = {}
        medias[media.mid[0]].id = media.mid[0]
        medias[media.mid[0]].createTime = media.c_time[0]
        medias[media.mid[0]].fileType = media.filetype[0]
        medias[media.mid[0]].mediaHeight = media.mh[0]
        medias[media.mid[0]].mlen = media.mlen[0]
        medias[media.mid[0]].mediaName = media.mname[0]
        medias[media.mid[0]].mediaSize = media.msize[0]
        medias[media.mid[0]].mediaTitle = media.mtitle[0]
        medias[media.mid[0]].mediaType = media.mtype[0]
        medias[media.mid[0]].mediaWidth = media.mw[0]
        medias[media.mid[0]].targetFolder = media.targetFolder[0]
      })
      return { mediaList, medias }
    }
    default:
      return state
  }
}

export default media