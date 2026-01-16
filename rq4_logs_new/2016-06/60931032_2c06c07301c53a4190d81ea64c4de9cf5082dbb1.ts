// design doc
// emit each tag as key
// emit title as value

interface NoteSchema {
  _id: string;
  _rev: string;
  title: string;
  content: string;
  tags: string[];
}

export default {

  _id: '_design/tags',

  views: {
    tags: {

      map: function (doc: NoteSchema) {
        if (doc.tags) {
          if (doc.tags.length === 0) {
            emit(null, {title: doc.title, tags: []});
          }
          else {
            for (var i = 0; i < doc.tags.length; i++) {
              emit(doc.tags[i], {title: doc.title, tags: doc.tags});
            }
          }
        }
      }.toString(),

      reduce: '_count'

    }
  }

}