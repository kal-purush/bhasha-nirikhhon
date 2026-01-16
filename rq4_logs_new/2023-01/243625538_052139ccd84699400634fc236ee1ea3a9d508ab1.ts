
/* Types */
import type { CmsConfig, CmsCollection, CmsField } from 'netlify-cms-core'

/* Field Type Definitions */
const TitleField: CmsField = { label: "Title", name: "title", widget: "string" };
const BodyField: CmsField = { label: "Body", name: "body", widget: "markdown" };
const SlugField: CmsField = { label: "Slug", name: "slug", widget: "string" };
const ThumbnailField: CmsField = { label: "Thumbnail", name: "thumbnail", widget: "image" };
const DateField: CmsField = { label: "Date", name: "date", widget: "datetime" };
const PostLayoutField: CmsField = { label: "Layout", name: "layout", widget: "hidden", default: "post" }

/**
 * Collections
 * 
 * @see https://www.netlifycms.org/docs/collection-types/
 */
const collections: CmsCollection[] = [
	{
		name: "posts",
		label: "Posts",
		label_singular: "Post",
		description: "Blog posts",
		folder: "content/posts",
		create: true,
		slug: "{{slug}}",
		fields: [PostLayoutField, TitleField, BodyField, SlugField, ThumbnailField, DateField]
	},
	{
		name: "service",
		label: "Services",
		label_singular: "Service",
		description: "Services",
		folder: "content/services",
		create: true,
		slug: "{{slug}}",
		fields: [TitleField, BodyField],
	},
	{
		name: "pages",
		label: "Pages",
		label_singular: "Page",
		description: "Pages",
		files: [
			{
				label: "Home",
				name: "home",
				file: "content/index.md",
				fields: [BodyField]
			},
			{
				label: "Privacy",
				name: "privacy",
				file: "content/privacy.md",
				fields: [BodyField]
			},
			{
				label: "Services",
				name: "services",
				file: "content/services.md",
				fields: [BodyField]
			},
			{
				label: "404",
				name: "404",
				file: "content/404.md",
				fields: [BodyField]
			}
		]
	}
]

/**
 * CMS Configuration
 * 
 * @see https://www.netlifycms.org/docs/configuration-options/
 */
export default {
	backend: {
		name: "github",
		branch: "staging",
		repo: "jdamner/amner.me"
	},
	logo_url: "/cms.png",
	site_url: "https://amner.me",
	media_folder: "public/assets",
	public_folder: "/assets",
	collections: collections
} as CmsConfig