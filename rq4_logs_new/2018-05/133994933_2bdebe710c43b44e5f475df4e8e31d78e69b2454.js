class DetailTimeline extends HTMLElement {

  createSvgEl(elName)
  {
    return document.createElementNS('http://www.w3.org/2000/svg', elName);
  }

  constructor()
  {
    super();

    const shadow = this.attachShadow({mode: 'open'});

    this.margin = {
      top: this.getAttribute('paddingTop'),
      right: this.getAttribute('paddingRight'),
      bottom: this.getAttribute('paddingBottom'),
      left: this.getAttribute('paddingLeft')
    };

    ///////////////////////////////////////////////////////////////
    this.width = this.getAttribute('width');
    this.height = this.getAttribute('height');
    const div = document.createElement('div');
    div.style.position = 'relative';

    this.svg = this.createSvgEl('svg');
    this.svg.setAttribute('width', this.width);
    this.svg.setAttribute('height', this.height);
    this.svg.style.position = 'absolute';
    this.svg.style.top = '0';
    this.svg.style.left = '0';

    this.svgContent = this.createSvgEl('g');
    this.svg.appendChild(this.svgContent);

    div.appendChild(this.svg);
    shadow.appendChild(div);

    const bcr = this.svg.getBoundingClientRect();
    this.width = bcr.width;
    this.height = bcr.height;

    const style = document.createElement('style');
    style.textContent =
        'svg { display: block; border: 1px solid rgba(200,200,200,0.5); }';
    //this.svg.appendChild(rect);

    shadow.appendChild(style);

    const d3script = document.createElement('script');
    d3script.src = 'https://d3js.org/d3.v4.min.js';

    const d3loaded = () =>
    {
      this.x = d3.scaleTime().range([this.margin.left, this.width - this.margin.right]);
      this.y = (value) => { return this.height - value * this.height };

      this.x.domain([this.start, this.end]);
      this.xAxis = d3.axisBottom(this.x);

      const context = d3.select(this.svg);


      this.axisEl = context.append("g")
          .attr("class", "axis axis--x")
          .attr("transform", "translate(0," + (this.height - this.margin.bottom) + ")");

      this.axisEl
          .call(this.xAxis);
    };

    d3script.onload = () => { d3loaded(); };
    shadow.appendChild(d3script);

  }

  updateVis()
  {
    if(this.x)
    {
      this.x.domain([this.start, this.end]);
      this.y = (value) => { return this.height / 2 - value * (this.height - this.margin.top - this.margin.bottom) / 2 };
      //this.xAxis = d3.axisBottom(this.x);

      this.axisEl
          .call(this.xAxis);

      this.redrawData()
    }
  }

  redrawData()
  {
    // Removing Old Data
    while (this.svgContent.firstChild)
    {
      this.svgContent.removeChild(this.svgContent.firstChild);
    }

    // Adding new data
    let d = 'M';
    const rate = (this.end - this.start) / 10000; // Sampling Rate - will cause overtones.

    for(let t = this.start; t <= this.end; t += rate)
    {
      d += this.x(t) + ' ' + this.y(Math.sin(t / 10)) + ' L';
    }
    d = d.substr(0, d.length - 1);

    const path = this.createSvgEl('path');
    path.setAttribute('d', d);
    path.setAttribute('stroke', '#d70');
    path.setAttribute('fill', 'none');

    this.svgContent.appendChild(path);
  }

  get start()
  {
    return parseInt(this.getAttribute('start'));
  }

  set start(start)
  {
    this.setAttribute('start', start);
  }

  get end()
  {
    return parseInt(this.getAttribute('end'));
  }

  set end(end)
  {
    this.setAttribute('end', end);
    this.updateVis();
  }
}

// Define the new element
customElements.define('detail-timeline', DetailTimeline);