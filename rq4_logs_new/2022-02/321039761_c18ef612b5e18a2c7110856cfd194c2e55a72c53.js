var Marks = {
    points: [],
    markPoint: function( char, tab, pos, url ) {
        this.points[char] = {tab: tab, pos: pos, url: url}
        console.log(char, url, pos);
        console.log(this.points);
    }
}