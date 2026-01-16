import {
	FindkitUI,
	html,
	preact,
} from "https://cdn.findkit.com/ui/v0.15.0/esm/index.js";

// https://docs.findkit.com/ui/slot-overrides/hooks#preact
const { useState } = preact;

const ui = new FindkitUI({
	publicToken: "<token>",
	// https://docs.findkit.com/ui/api/#minTerms
	minTerms: 0,
	// https://docs.findkit.com/ui/styling
	css: `
		.product-image {
			height: 200px;
			width: 200px;
			object-fit: contain;
		}

		.add-to-cart {
			width: 100px
		}

	`,
	params: {
		// https://docs.findkit.com/ui/filtering/
		filter: {
			// https://docs.findkit.com/ui/filtering/operators#$exists
			productPrice: { $exists: true },
		},
	},
	// https://docs.findkit.com/ui/slot-overrides/
	slots: { Hit },
});

// https://docs.findkit.com/ui/api/#openFrom
ui.openFrom("button.search");

function Hit(props) {
	// https://docs.findkit.com/ui/slot-overrides/slots#hit
	const { TitleLink, Highlight } = props.parts;
	const price = props.hit.customFields.productPrice?.value;
	const description = props.hit.customFields.productDescription?.value;
	const image = props.hit.customFields.productImageURL?.value;
	const productId = props.hit.customFields.productId?.value;

	return html`
		<!-- Render the original title link -->
		<${TitleLink} />

		<!-- show the product image if it exists -->
		${image &&
			html`
				<img class="product-image" src=${image} />
			`}

		<!-- Show highlight only when the user has searched for
         something, otherwise fallback to showing the description -->
		${props.hit.highlight
			? html`
					<${Highlight} />
			  `
			: html`
					<div class="product-description">
						${description}
					</div>
			  `}

		<div class="product-price">Price: ${price}</div>

		<${AddToCart} productId=${productId} />
	`;
}

/**
 * Add a product by the product id to the cart using the WooCommerce AJAX API.
 */
async function addToCart(productId) {
	// wc_add_to_cart_params is javascript global set by WooCommerce which
	// contains infmation how to handle the cart
	if (typeof wc_add_to_cart_params === "undefined") {
		throw new Error(
			"the wc_add_to_cart_params global is undefined, cannot add to cart",
		);
	}

	const data = new FormData();
	data.append("product_id", productId);

	const res = await fetch(
		wc_add_to_cart_params.wc_ajax_url.replace("%%endpoint%%", "add_to_cart"),
		{ method: "POST", body: data },
	);

	if (!res.ok || res.status !== 200) {
		throw new Error("Failed to add to cart");
	}

	const resData = await res.json();

	if (resData.error) {
		throw new Error("Failed to add to cart");
	}
}

/**
 * A button that adds a product to the cart.
 */
function AddToCart(props) {
	const { productId } = props;
	const [status, setStatus] = useState("initial");

	const onClick = async () => {
		if (status !== "initial") {
			return;
		}

		setStatus("adding");

		try {
			await addToCart(productId);
			setStatus("added");
		} catch (error) {
			console.error("Failed to add to cart", error);
			setStatus("error");
		}
	};

	if (status === "error") {
		return html`
			<div class="error">Failed to add to cart</div>
		`;
	}

	if (status === "added") {
		return html`
			<div>
				Added to cart.
				<a href=${wc_add_to_cart_params.cart_url}>View cart.</a>
			</div>
		`;
	}

	return html`
		<button
			class="add-to-cart"
			disabled=${status !== "initial"}
			type="button"
			onClick=${onClick}
		>
			${status === "initial" ? "Add to Cart" : "Adding..."}
		</button>
	`;
}