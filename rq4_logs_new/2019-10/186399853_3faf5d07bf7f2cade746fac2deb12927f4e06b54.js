const Store = require("electron-store");
const sites = [
  {
    class: "youtube",
    url: "https://www.youtube.com/",
    name: "Youtube"
  },
  {
    class: "netflix",
    url: "https://www.netflix.com/",
    name: "Netflix"
  },
  {
    class: "fz",
    url: "https://www.123moviesgot.com/",
    name: "123moviesgot"
  },
  {
    class: "vimeo",
    url: "https://www.vimeo.com/",
    name: "Vimeo"
  },
  {
    class: "plural",
    url: "https://www.pluralsight.com/",
    name: "PluralSight"
  },
  {
    class: "tree",
    url: "https://www.teamtreehouse.com/",
    name: "Tree House"
  },
  {
    class: "xpause",
    url: "https://www.egghead.io/",
    name: "Egghead.io"
  },
  {
    class: "fz",
    url: "https://www.fzmovies.net/",
    name: "FZMovies"
  },
  {
    class: "vimeo",
    url: "https://www.khanacademy.com/",
    name: "Khan Academey"
  },
  {
    class: "facebook",
    url: "https://www.facebook.com/",
    name: "Facebook"
  },
  {
    class: "twitter",
    url: "https://www.twitter.com/",
    name: "Twitter"
  },
  {
    class: "xpause",
    url: "https://www.xpau.se/",
    name: "Xpau.se"
  }
];

class DataStore extends Store {
  constructor(settings) {
    super(settings);

    this.sites = this.get("sites") || sites;
    this.history = this.get("history") || [];
  }

  saveSites() {
    this.set("sites", this.sites);
    return this;
  }

  getSites() {
    this.sites = this.get("sites") || [];
    return this;
  }

  addSite(site) {
    this.sites = [...this.sites, site];
    return this.saveSites();
  }
  deleteSite(site) {
    this.sites = this.sites.filter(s => s !== site);
    return this.saveSites();
  }
}

module.exports = DataStore;