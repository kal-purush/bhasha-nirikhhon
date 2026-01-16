// -------------
// SUPPORT CODE
// -------------
class ScreenGrabber {
    constructor(regionId, downloadButtonId) {
        this.region = document.getElementById(regionId);
        this.downloadButton = document.getElementById(downloadButtonId);
        this.canvas = {};
    }

    download = function() {
        this.downloadButton.className = "hidden";
        this.canvas = html2canvas(this.region).then(function(canvas) {
            const a = document.createElement('a');
            a.href = canvas.toDataURL("image/png").replace("image/png", "image/octet-stream");
            a.download = 'blanket.png';
            a.click();
        });
        this.downloadButton.className = "";
    }
}

class TextFiller {
    constructor(sourceId, targetId) {
        this.source = document.getElementById(sourceId);
        this.target = document.getElementById(targetId);
    }

    fill() {
        const sourceText = this.source.textContent;
        this.target.innerText = this.source.textContent;
    }

    empty() {
        this.target.innerText = "";
    }
}

class ClipboardCopier {
    colors = [];
    textToCopy = "";

    constructor(colors) {
        this.colors = colors;
        let i = 0;

        while (i < colors.length) {
            const color = colors[i];
            this.textToCopy += color.displayName + "\n" + color.numberSmall + " small\t" + color.numberLarge + " large\n\n";
            i++;
        }
    }

    copy() {
        navigator.clipboard.writeText(this.textToCopy)
            .then(() => { alert(`Copied!`); })
            .catch((error) => { alert(`Copy failed! ${error}`); })
    }
}

class SizeTracker {
    numberOfColumns = 0;
    sizesArray = [];
    totalSmall = 0;
    totalLarge = 0;

    constructor(numberOfColumns) {
        this.numberOfColumns = numberOfColumns;
    }

    checkSizesArray(className) {
        if (className == "large-circle" && this.sizesArray.length > 0) {
            let index = this.sizesArray.length;
            if (this.sizesArray[index - 1] == "large-circle") { className = "small-circle"; }
    
            if (this.sizesArray.length > this.numberOfColumns) {
                if ((this.sizesArray[index - (this.numberOfColumns - 3)] == "large-circle") || 
                    (this.sizesArray[index - (this.numberOfColumns - 2)] == "large-circle") ||
                    (this.sizesArray[index - (this.numberOfColumns - 1)] == "large-circle") ||
                    (this.sizesArray[index - (this.numberOfColumns)] == "large-circle") ||
                    (this.sizesArray[index - (this.numberOfColumns + 1)] == "large-circle")) { 
                    className = "small-circle"; 
                }            
            }
        }
    
        this.sizesArray.push(className);
        return className;
    }

    addToTotal(className) {
        if (className == "small-circle") { this.totalSmall++; }
        else { this.totalLarge++; }
    }

    getTotals() {
        return { small: this.totalSmall, large: this.totalLarge, total: this.totalLarge + this.totalSmall };
    }
}

class ColorTracker {
    colors = [
        { name: "stormBlue", value: "91, 133, 148", displayName: "Storm Blue", numberSmall: 0, numberLarge: 0 },
        { name: "cream", value: "244, 228, 217", displayName: "Cream", numberSmall: 0, numberLarge: 0 },
        { name: "petrol", value: "12, 67, 92", displayName: "Petrol", numberSmall: 0, numberLarge: 0 },
        { name: "tomato", value: "140, 16, 16", displayName: "Tomato", numberSmall: 0, numberLarge: 0 },
        { name: "graphite", value: "65, 64, 66", displayName: "Graphite", numberSmall: 0, numberLarge: 0 },
        { name: "teal", value: "15, 101, 121", displayName: "Teal", numberSmall: 0, numberLarge: 0 },
        { name: "daffodil", value: "251, 220, 150", displayName: "Daffodil", numberSmall: 0, numberLarge: 0 }
    ];
    colorIndex = 0;

    constructor() {
    }

    getColors() {
        return this.colors;
    }

    getNextColor() {
        let color = this.colors[this.colorIndex];

        if (this.colorIndex < this.colors.length - 1) {
            this.colorIndex++;
        } else {
            this.colorIndex = 0;
        }
        return(color);
    }

    addToTotal(color, className) {
        let colorToModify = this.colors[this.colors.indexOf(color)];
        if (className == "small-circle") { colorToModify.numberSmall++; }
        else { colorToModify.numberLarge++; }
    }
}

// -------------
// GLOBALS
// -------------
const globals = {
    blanketDivId: "blanket",
    blanketPreviewContainerDivId: "blanketPreviewContainer",
    clipboardId: "clipboard",
    colorContainerDivId: "colorContainer",
    columnsDefault: 11,
    downloadButtonDivId: "downloadButtonContainer",
    exportContainerDivId: "exportContainer",
    rowsDefault: 12,
    smallProbability: .5,
    squareColor: { name: "duckEgg", value: "219, 236, 235", displayName: "Duck Egg" }
};

// ----------------
// REACT COMPONENTS
// ----------------
var ColorTotal = React.createClass({
    render: function() {
        return(
            <div>
            <hr />
            <h4>{this.props.displayName}: {this.props.number}</h4>
            <p>Small: {this.props.numberSmall}, Large: {this.props.numberLarge}</p>
            </div>
        )
    }
})

var ColorEntry = React.createClass({
    render: function() {
        return(
            <ul className="list-unstyled">
                <li>
                    <b>{this.props.displayName}</b><br />
                    Small: {this.props.numberSmall}, Large: {this.props.numberLarge}
                </li>
                {/* <h6></h6> 
                <p></p> */}
            </ul>
        )
    }
})

var ColorList = React.createClass({
    render: function() {
        let colors = this.props.colors;
        let colorEntries = [];
        let i = 0;

        while (i < colors.length) {
            colorEntries.push(
                <ColorEntry 
                    key={i} 
                    displayName={colors[i].displayName} 
                    numberSmall={colors[i].numberSmall} 
                    numberLarge={colors[i].numberLarge} />
            );
            i++;
        }
        colorEntries.push(
            <ColorTotal 
            key={99} 
            displayName="Total" 
            number={sizeTracker.getTotals().total}
            numberSmall={sizeTracker.getTotals().small} 
            numberLarge={sizeTracker.getTotals().large} />
        );

        return (
            <div>{colorEntries}</div>
        )
    }
})

var BlanketCircle = React.createClass({
    render: function() {
        let smallSize = (Math.random() < globals.smallProbability);
        let className = (smallSize) ? "small-circle" : "large-circle";
        className = sizeTracker.checkSizesArray(className);
        let circleColor = colorTracker.getNextColor();
        let circleColorValue = circleColor.value;
        let circleBackgroundColor = "rgb(" + circleColorValue + ")";
        sizeTracker.addToTotal(className);
        colorTracker.addToTotal(circleColor, className);
        return (
            <span className={className} style={{ backgroundColor: circleBackgroundColor }} ></span>
        )
    }
})

var BlanketSquare = React.createClass({
    render: function() {
        let squareBackgroundColor = "rgb(" + globals.squareColor.value + ")";
        return (
            <div className="blanket-square" style={{ backgroundColor: squareBackgroundColor }}>
                <BlanketCircle />
            </div>
        )
    }
})

var BlanketRow = React.createClass({
    render: function() {
        let blanketSquares = [];
        let i = 0;

        while (i< this.props.columns) {
            blanketSquares.push(<BlanketSquare key={i} />);
            i++;
        }

        return ( 
            <div className="blanket-row">
                {blanketSquares}
            </div>
        )
    }
});

var DownloadButton = React.createClass({
    handleDownloadClick: function(e) {
        e.preventDefault();
        const screenGrabber = new ScreenGrabber(globals.exportContainerDivId, globals.downloadButtonDivId);
        screenGrabber.download();
    },
    render: function() {
        return (            
            <a className="btn btn-primary btn-sm" id="downloadButton" href="#" role="button" onClick={this.handleDownloadClick}>Download &amp; Copy</a>
        )
    }
})

var Blanket = React.createClass({
    render: function() {
        let blanketRows = [];
        let i = 0;

        while (i < this.props.rows) {
            blanketRows.push(<BlanketRow key={i} colors={this.props.colors} columns={this.props.columns} />);
            i++;
        }

        return ( 
            <div id={globals.blanketDivId}>
                {blanketRows}
            </div>
        )
    }
});

var Form = React.createClass({
    getInitialState: function() {
        return {
            colors: colorTracker.getColors(),
            rows: globals.rowsDefault,
            columns: globals.columnsDefault,
        };
    },
    componentDidMount: function() {
        this.displayResults();
    },
    displayResults: function() {
        ReactDOM.render(
            <ColorList colors={this.state.colors} />, document.getElementById(globals.colorContainerDivId)
        );
        ReactDOM.render(
            <DownloadButton />, document.getElementById(globals.downloadButtonDivId)
        );
    },
    render: function() {
        return (
            <div>
                <Blanket colors={this.state.colors} rows={this.state.rows} columns={this.state.columns} />
            </div>
        );
    }
});

// ----------------
// Start the show
// ----------------
const colorTracker = new ColorTracker();
const sizeTracker = new SizeTracker(globals.columnsDefault);
ReactDOM.render(<Form />, document.getElementById(globals.blanketPreviewContainerDivId));