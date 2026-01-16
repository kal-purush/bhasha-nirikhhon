var textEditor = {
    editor:null,

    setId: function(id){
        if(!this.editor && id){
            this.editor = $("#" + id);
        }
    },


    setIcon:function(editor, icon){
        this.setId(editor);
        icon = $("<i>", {
            class: "glyphicon glyphicon-" + icon
        });

        this.editor.append(icon);

    },

    // events
    keydown: function (t) {
        this.setId(t.id);
        d("keydown = ");
    },
    keyup: function (t) {
        this.setId(t.id);
        d("keyup = ");
    },
    keypress: function (t) {
        this.setId(t.id);
        d("keypress = ");
    }
};