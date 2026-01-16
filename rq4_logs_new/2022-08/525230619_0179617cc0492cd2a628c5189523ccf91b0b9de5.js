
(async function () {

    const BUSINESS_ID = "62024b7c0d36dfc17e75e2bd";
    // const BUSINESS_ID = "61a749c95eb0a0d5225e4572";
    const LOGO = 'logo.webp';
    const HEADER_LOGO_IMAGAE = `https://www.soppiya.com/media/images/${BUSINESS_ID}/business/${LOGO}`;
    const PRIMARY_COLOR = `#1E272E`;
    typeof handleNotification === "function" && handleNotification(true, true);

    // handle header logo
    async function headerLogo() {
        const themeLogo = document.getElementById("s1401_header_logo_id");
        themeLogo.setAttribute("src", `${HEADER_LOGO_IMAGAE}`);
        themeLogo.addEventListener("click", function () {
            typeof handleNavigate == "function" && handleNavigate("/");
            console.log("Home Page");
        })
    }
    await headerLogo();

    // handle open close sidebar
    const hamburgerIcon = document.querySelectorAll(".s1401_open_sidebar_id");
    const sidebarParent = document.getElementById("s1401_sidebar_parent");
    const sidebarCloseIcon = document.querySelector(".s1401_sidebar_close_btn");
    const sidebarContentDiv = document.querySelector(".s1401_sidebar_content_wrapper");
    async function handleOpenSidebar() {
        for (singleIcon of hamburgerIcon) {
            singleIcon.addEventListener("click", function () {
                sidebarParent.classList.add("s1401_open_sidebar");
            });
        };
    };
    await handleOpenSidebar();

    // remove sidebar
    async function removeSidebar() {
        sidebarCloseIcon.addEventListener("click", function () {
            sidebarParent.classList.remove("s1401_open_sidebar");
        });
    };
    await removeSidebar();

    async function sidebarRemoveOutSideClick() {
        sidebarParent.addEventListener("click", function (e) {
            const targetDiv = sidebarContentDiv.contains(e.target);
            if (!targetDiv) {
                sidebarParent.classList.remove("s1401_open_sidebar");
            }
        });
    };
    await sidebarRemoveOutSideClick();

    // show sidebar social icon 

    const apiCall = async (url) => { try { let response = await fetch(url, { method: "get", headers: { "businessid": `${BUSINESS_ID}` } }); response = await response.json(); if (response.Error) { return console.log(response.Error) }; return response; } catch (e) { return }; };

    const socialData = await apiCall(`https://api.soppiya.com/v2.1/widget/header/social`);
    const showSocialIcon = async (socialData) => {
        for (let i = 0; i < socialData.length; i++) {
            const element = socialData[i];
            const IconLi = elementMaker("li", ["s001_social_nav_item"]);
            const IconLink = elementMaker("a", ["s001_social_nav_link"]);
            IconLink.setAttribute("src", `${element.url}`);
            IconLink.innerHTML = `${element.svg}`;
            IconLi.appendChild(IconLink);
            IconLink.children[0].children[0].lastChild.style.fill = `${PRIMARY_COLOR}`;
            document.getElementById("s001_social_nav_list_id").appendChild(IconLi);
            IconLi.addEventListener("click", () => {
                window.open(`${element.url}`, "_blank");
                console.log("url link", `${element.url}`);
            });
        };
    };
    showSocialIcon(socialData);

    // show legal page
    const legalPages = await apiCall(`https://api.soppiya.com/v2.1/widget/header/page`);
    // console.log("legalPages", legalPages);
    const showLegalPage = async (legalPages) => {
        for (let i = 0; i < legalPages.length; i++) {
            const element = legalPages[i];
            let pageLi = elementMaker("li", ["s1401_sidebar_menu_list"]);
            let InnerSpan = elementMaker("span", ["s1401_legal_nav_link"]);
            InnerSpan.innerText = `${element.title}`;
            pageLi.appendChild(InnerSpan);
            document.getElementById("s1401_legal_pages_wrapper_id").appendChild(pageLi);
            pageLi.addEventListener("click", function () {
                typeof handleNavigate === "function" && handleNavigate(`/page/${element.slug}`);
                console.log(`page/${element.slug}`);
            });

        }
    }
    await showLegalPage(legalPages);


    // handle sideba catagories
    const allCatagoriesTitle = document.getElementById("s1401_catagories_all_title");

    allCatagoriesTitle.addEventListener("click", function () {
        event.stopImmediatePropagation();
        allCatagoriesTitle.classList.toggle("s1401_active_menu");
    });


    // sub catagories
    const loadAllCatagories = await apiCall(`https://api.soppiya.com/v2.1/widget/header/category`);
    const showCatagories = async (loadAllCatagories) => {
        for (let i = 0; i < loadAllCatagories.length; i++) {
            const element = loadAllCatagories[i];
            // console.log("catagories", element);
            const catagoriesLi = elementMaker("li", ["s1401_sub_catagories_list"]);
            const catagoriesContentWrapper = elementMaker("div", ["s1401_sub_catagories_list_content"]);
            catagoriesLi.appendChild(catagoriesContentWrapper);
            const catagiresName = elementMaker("span", ["s1401_sub_catagories_name"]);
            catagiresName.innerText = `${element.name}`;
            catagoriesContentWrapper.appendChild(catagiresName);
            document.getElementById("s1401_sub_catagories_list_wrapper_id").appendChild(catagoriesLi);
            const catagoriesArrowIcon = elementMaker("span", ["s1401_open_sub_menu_icon"]);
            catagoriesArrowIcon.innerHTML = `
           <svg xmlns="http://www.w3.org/2000/svg" width="8" height="4.349" viewBox="0 0 8 4.349">
                                                <path id="Path_1732" data-name="Path 1732" d="M4,4.349A1.189,1.189,0,0,1,3.157,4L0,.843.843,0,4,3.157,7.157,0,8,.843,4.843,4A1.189,1.189,0,0,1,4,4.349Z" opacity="0.3"></path>
                                            </svg>
           `;

            if (element.hasChild == true) {
                catagoriesContentWrapper.appendChild(catagoriesArrowIcon);
                catagoriesLi.addEventListener("click", async function () {
                    event.stopImmediatePropagation();
                    const elementId = element._id;

                    if (catagoriesLi.children.length === 1) {
                        catagoriesLi.classList.add("s1401_active_menu");
                        await subSubCatagories(elementId, catagoriesLi);

                    } else {
                        catagoriesLi.classList.toggle("s1401_active_menu");
                    }

                    /* catagoriesLi.classList.add("s1401_active_menu");
                    await subSubCatagories(elementId, catagoriesLi); */
                });
            } else {
                catagoriesLi.addEventListener("click", function () {
                    event.stopImmediatePropagation();
                    typeof handleNavigate === "function" && handleNavigate(`/category/${element._id}`);
                    console.log(`/category/${element._id}`);
                });
            };

        };
    };
    await showCatagories(loadAllCatagories);





    // handle sub sub catagories

    async function subSubCatagories(elementId, parentLi) {
        const subSubData = await apiCall(`https://api.soppiya.com/v2.1/widget/header/category/${elementId}`);
        const subSubWrapper = elementMaker("ul", ["s1401_sub_sub_catagories_list_wrapper"]);
        for (let i = 0; i < subSubData.length; i++) {
            const element = subSubData[i];
            // console.log("SubSubData", element);
            const subSubLi = elementMaker("li", ["s1401_sub_sub_catagories_list"]);
            const subSubCatagoriesConent_wrapper = elementMaker("div", ["s1401_sub_sub_catagories_list_content"]);
            subSubLi.appendChild(subSubCatagoriesConent_wrapper);
            const subsubCatagoriesName = elementMaker("span", ["s1401_sub_sub_catagories_name"]);
            subsubCatagoriesName.innerText = `${element.name}`;
            subSubCatagoriesConent_wrapper.appendChild(subsubCatagoriesName);
            subSubWrapper.appendChild(subSubLi);
            const catagoriesArrowIcon = elementMaker("span", ["s1401_open_sub_sub_menu_icon"]);
            catagoriesArrowIcon.innerHTML = `
            <svg
                                                        xmlns="http://www.w3.org/2000/svg" width="8"
                                                        height="4.349" viewBox="0 0 8 4.349">
                                                        <path id="Path_1732" data-name="Path 1732"
                            d="M4,4.349A1.189,1.189,0,0,1,3.157,4L0,.843.843,0,4,3.157,7.157,0,8,.843,4.843,4A1.189,1.189,0,0,1,4,4.349Z"
                                                            opacity="0.3" />
                                                    </svg>
            `;
            if (element.hasChild == true) {
                subSubCatagoriesConent_wrapper.appendChild(catagoriesArrowIcon);
                subSubLi.addEventListener("click", async function () {
                    event.stopImmediatePropagation();
                    const elementId = element._id;

                    if (subSubLi.children.length === 1) {
                        subSubLi.classList.add("s1401_active_menu");
                        await subSubSubCatagories(elementId, subSubLi);

                    } else {
                        subSubLi.classList.toggle("s1401_active_menu");
                    }

                    /* subSubLi.classList.add("s1401_active_menu");
                    await subSubSubCatagories(subSubLi, elementId); */
                });
            } else {
                subSubLi.addEventListener("click", function () {
                    event.stopImmediatePropagation();
                    typeof handleNavigate === "function" && handleNavigate(`/category/${element._id}`);
                    console.log(`/category/${element._id}`);
                });
            };
        };
        parentLi.appendChild(subSubWrapper);

    };


    // sub sub sub handle
    async function subSubSubCatagories(elementId, parenLi) {
        const subSubSubData = await apiCall(`https://api.soppiya.com/v2.1/widget/header/category/${elementId}`);
        // console.log("subSubSubData",subSubSubData);
        const subsubsubWrapper = elementMaker("ul", ["s1401_sub_sub_sub_catagories_list_wrapper"]);
        for (let i = 0; i < subSubSubData.length; i++) {
            const element = subSubSubData[i];
            // console.log("subSubSubData",element);
            const subsubsubLi = elementMaker("li", ["s1401_sub_sub_sub_catagories_list"]);
            const subsubsubContentWrapper = elementMaker("div", ["s1401_sub_sub_sub_catagories_list_content"]);
            subsubsubLi.appendChild(subsubsubContentWrapper);
            const subsubsubName = elementMaker("span", ["s1401_sub_sub_sub_catagories_name"]);
            subsubsubName.innerText = `${element.name}`;
            subsubsubContentWrapper.appendChild(subsubsubName);
            subsubsubWrapper.appendChild(subsubsubLi);
            subsubsubLi.addEventListener("click", function () {
                event.stopImmediatePropagation();
                typeof handleNavigate === "function" && handleNavigate(`/category/${element._id}`);
                console.log(`/category/${element._id}`);
            });
        };
        parenLi.appendChild(subsubsubWrapper);
    };


    // handle wishlist cart click hnadler
    async function cartWishlistClickHandler() {

        document.getElementById("s1401_header_wishlist_id").addEventListener("click", function () { cartWishlistNavigate("s1401_header_wishlist_id", "s1401_mobile_wishlist_id", "/wishlist") });
        document.getElementById("s1401_mobile_wishlist_id").addEventListener("click", function () { cartWishlistNavigate("s1401_header_wishlist_id", "s1401_mobile_wishlist_id", "/wishlist") });

        document.getElementById("s1401_header_cart_id").addEventListener("click", function () { cartWishlistNavigate("s1401_header_cart_id", "s1401_mobile_cart_id", "/cart") });
        document.getElementById("s1401_mobile_cart_id").addEventListener("click", function () { cartWishlistNavigate("s1401_header_cart_id", "s1401_mobile_cart_id", "/cart") });
    }
    await cartWishlistClickHandler();

    // cart wishlist set route
    function cartWishlistNavigate(headerID, mobileID, path) {
        typeof handleNavigate === "function" && handleNavigate(`${path}`);
        removeActiveFill();
        document.getElementById(`${headerID}`).classList.add("s1401_active_menu_icon");
        document.getElementById(`${mobileID}`).classList.add("s1401_active_menu_icon");
        console.log(`${path}`);
    };

    async function removeActiveFill() {
        const cartWishlistIcon = document.querySelectorAll(".s1401_menu_list_item");
        for (let i = 0; i < cartWishlistIcon.length; i++) {
            const element = cartWishlistIcon[i];
            element.classList.contains("s1401_active_menu_icon") && element.classList.remove("s1401_active_menu_icon");
        };
    };

    // notification badget handle






    function elementMaker(name, className, id) {
        try {
            let element = document.createElement(name);
            className && (element.className = className.join(" "));
            id && (element.id = id);
            return element;
        } catch (err) { };
    };
    function setAttributes(elementName, allAttributes) {
        for (let key in allAttributes) {
            elementName.setAttribute(key, allAttributes[key]);
        };
    };
})();