let body = d3.select("body")
let info = body.append("div").attr("id", "info")
// add title
info.append("h2")
    .text("Very interesting Chart about movies")
    .attr("id", "title")

//add description
info.append("div")
    .text("It shows the most popular movies")
    .attr("id", "description")

//create tooltip

let tooltip = body.append("div")
    .attr("id", "tooltip")
    .style("opacity", "0")


//create treemap and legend
let h = window.innerHeight
let w = h

let svg = body.append("svg")
    .attr("id", "graph")
    .attr("width", w)
    .attr("height", h)
d3.json("https://cdn.freecodecamp.org/testable-projects-fcc/data/tree_map/movie-data.json")
.then(data=>{
    //create treemap
    let indexArr = data.children.map(a=>data.children.indexOf(a))
    let namesArr = data.children.map(a=>a.name)
    let color = d3.scaleLinear()
        .domain([d3.min(indexArr), d3.max(indexArr)])
        .range(["black", "rgb(179, 224, 238)"])

    let root = d3.hierarchy(data).sum(d=>d.value)

    d3.treemap()
        .size([w, h])
        .padding(0)
        (root)

    //append rect elements
    svg.selectAll("rect")
        .data(root.leaves())
        .enter()
        .append("rect")
        .attr("class", "tile")
        .attr("x", d=>d.x0)
        .attr("y", d=>d.y0)
        .attr("width", d=>d.x1-d.x0-1)
        .attr("height", d=>d.y1-d.y0-1)
        .attr("fill", d=>color(namesArr.indexOf(d.data.category)))
        .attr("data-name", d=>d.data.name)
        .attr("data-category", d=>d.data.category)
        .attr("data-value", d=>d.data.value)
        .on("mouseover", e=>{
            let attr = e.target.attributes
            tooltip
                .style("opacity", "1")
                .attr("data-value", attr["data-value"].value)
                .text(`${attr["data-category"].value} \n ${attr["data-name"].value} \n ${attr["data-value"].value}`)

        })
        .on("mousemove", e=>{
            let x = e.clientX
            let y = e.clientY
            tooltip
                .attr("style",`left: ${x+10}px; top: ${y+10}px`)
        })
        .on("mouseout", e=>{
            tooltip
                .style("opacity", "0")
                .attr("data-value", "")
                .text("")
        })

    let textOrient = (d) => {
        let w = d.x1-d.x0
        let h = d.y1-d.y0
        let style = ""
        if (h>w) {
            style = "writing-mode: vertical-rl;"
        }
        return style
    }
    svg.selectAll("text")
        .data(root.leaves())
        .enter()
        .append("text")
        .attr("x", d=>d.x0+20)
        .attr("y", d=>d.y0+20)
        .attr("style",d=>textOrient(d))
        .attr("class", "text")
        .text(d=>d.data.value)

    //create legend
    let padding = 70
    let lh = 300
    let lw = padding + lh/7
    let legend = body.append("svg")
        .attr("id", "legend")
        .attr("width", lw)
        .attr("height", lh)
    let yScale = d3.scalePoint()
        .domain(namesArr)
        .range([0, lh-20])
    let yAxis = d3.axisLeft(yScale)
    legend.selectAll("rect")
        .data(namesArr)
        .enter()
        .append("rect")
        .attr("class", "legend-item")
        .attr("x", padding)
        .attr("y", (d, i)=>i*(lh/7))
        .attr("width", d=>(lh/7))
        .attr("height", d=>((lh/7)))
        .attr("fill", d=>color(namesArr.indexOf(d)))

    legend.append("g")
        .attr("transform", "translate("+ padding +", "+ 10 + ")")
        .call(yAxis)
})