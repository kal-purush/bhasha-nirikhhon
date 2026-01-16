import GenericPortal from "./generic_portal";
import PortalInterface from "../interfaces/portal_interface";

class G1Portal extends GenericPortal implements PortalInterface {
  constructor() {
    super();
    this.set_url("https://g1.globo.com");
    this.set_articles_query("div.feed-post");

    this.set_title({
      query: "p",
      attribute: "text",
    });
    this.set_date({
      query: "div.feed-post-metadata",
      attribute: "text",
    });
    this.set_font("Não definida ainda");
    this.set_image({
      query: ".bstn-fd-picture-image",
      attribute: "src",
    });
    this.set_url_new({
      query: "a",
      attribute: "href",
    });
  }
}

export default G1Portal;