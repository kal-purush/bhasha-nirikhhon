class DeviceMetric {
  constructor(metric_key, metric_value, metric_human_name, icon, metric_unit) {
    this.metric_key = metric_key;
    this.metric_value = metric_value;
    this.metric_human_name = metric_human_name;
    this.metric_unit = metric_unit || '';
    this.icon = icon;
  }

  render(container = null) {
    var html = `
      <div class="device-metric col s6 m3" data-metric="${this.metric_key}">
        <div class="card horizontal">
          <div class="card-image icon">
            <i class="material-icons">${this.icon}</i>
          </div>
          <div class="card-stacked">
            <div class="card-content">
              <div class="name">${this.metric_human_name}</div>
              <div class="card-title value">${this.metric_value}</div>
              <div class="unit">${this.metric_unit}</div>
            </div>
          </div>
        </div>
      </div>
    `;

    if (container) {
      container.insertAdjacentHTML('afterbegin', html);
    }

    return html;
  }
}