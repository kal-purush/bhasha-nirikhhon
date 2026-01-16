import { siteConfig } from "@/lib/config";
import { getBaseUrl } from "@/lib/utils";
import { type MetadataRoute } from "next";

export default function robots(): MetadataRoute.Robots {
	return {
		rules: {
			userAgent: "*",
			allow: "/",
			disallow: ["/thank-you", "/api/", "/admin/", "/order", "/order/"],
		},
		sitemap: getBaseUrl(siteConfig.url, "/sitemap.xml"),
	};
}