import {
    showJavascript, 
    showPython, 
    showReactJS, 
    showCorporate, 
    showTikTok, 
    showSales, 
    showBanking, 
    showCopywriting,
    showPresentation,
    showLightingManufacturer,
    showPlasticManufacturer,
    showBasePrinciple,
    showLumbermart,
    showBackley

} from './skillModal.js';

import { createShareModal } from './shareModal.js';

const textData = [
    {   
        header: "Jo-Hann's Hamburger Portfolio", 
        subheader: "Scroll Down or Press Down Arrow To Begin",
        images: [],
        buttons: []
    },
    { 
        header: "Programming Patty", 
        subheader: "Rich, Grounded and Substance-driven. Transforming challenges into clean, effective code.",
        images: [
            { src: "./assets/programming/popnik.jpg", alt: "ReactJS" },
            { src: "./assets/programming/perlinwave.png", alt: "Javascript" },
            { src: "./assets/programming/facetracking.png", alt: "Python" },
        ],
        buttons: [
            { text: "React JS", action: showReactJS },
            { text: "Javascript", action: showJavascript },
            { text: "Python", action: showPython },
        ]
    },
    { 
        header: "Videography Veggie", 
        subheader: "Layered, Crisp, and Refreshing. Bringing clarity and context, ensuring every story feels alive.",
        images: [
            { src: "./assets/videography/corporate.jpg", alt: "Yellow Group" },
            { src: "./assets/videography/tiktok2.png", alt: "Cofundr" },
            { src: "./assets/videography/sales.jpg", alt: "Ninso Malaysia" },
        ],
        buttons: [
            { text: "Corporate", action: showCorporate },
            { text: "TikTok", action: showTikTok },
            { text: "Sales", action: showSales },
        ]
    },
    { 
        header: "Tactical Tomato", 
        subheader: "Bright, Bold, and Impactful. Cutting through challenges with precision and versatility.",
        images: [
            { src: "./assets/tactical/lighting.png", alt: "Evergrown" },
            { src: "./assets/tactical/plastic.png", alt: "SPS Plas " },
        ],
        buttons: [
            { text: "Lighting", action: showLightingManufacturer },
            { text: "Plastic", action: showPlasticManufacturer },
        ]
    },
    { 
        header: "Communication Cheese", 
        subheader: "Melty, Smooth, and Adaptable. The layer that builds trust and makes everything stick together.",
        images: [
            { src: "./assets/communication/copywriting.jpg", alt: "Acacia Fabrics" },
            { src: "./assets/communication/presentation.jpg", alt: "Quarters Malaysia" },
        ],
        buttons: [
            { text: "Copywriting", action: showCopywriting },
            { text: "Presentation", action: showPresentation }
        ]
    },
    { 
        header: "Base Buns", 
        subheader: "A sturdy sourdough, slow-fermented for strength and character. It’s reliable, and always here to support.",
        images: [
            { src: "./assets/principle/principle.png", alt: "Solve + Tell = Sell" },
        ],
        buttons: [
            { text: "Philosophy", action: showBasePrinciple }
        ]
    },
    { 
        header: "Friendly Fries", 
        subheader: "Fries are the ultimate team player—they shine on their own but thrive when paired with the main act",
        images: [
            { src: "./assets/tactical/banking2.jpg", alt: "CIMB" },
        ],
        buttons: [
            { text: "Banking", action: showBanking }
        ]
    },
    { 
        header: "Dynamic Drink", 
        subheader: "Cool, refreshing, and thirst-quenching. Just like my ads, they highlight the best in every sip.",
        images: [
            { src: "./assets/dynamic/lumbermart.jpg", alt: "Lumbermart" },
            { src: "./assets/dynamic/backley.jpg", alt: "Backley" },
        ],
        buttons: [
            { text: "Flooring", action: showLumbermart },
            { text: "Homeware", action: showBackley }
        ]
    },
    {
        header: "The Ultimate Burger", 
        subheader: "Stacked and Ready To Serve!",
        images: [
            { src: "./assets/share/linkedin.jpg", alt: "Linkedin" },
            { src: "./assets/share/share.png", alt: "Linkedin" },
        ],
        buttons: [
            {
                text: "LinkedIn",
                action: () => window.open(
                    `https://www.linkedin.com/in/jo-hann-wong-164510225/`,
                    '_blank'
                )
            },
            {
                text: "Share",
                action: createShareModal
            }
            
        ]
    }
];

export function mainMenu(textContainerId) {
    const textContainer = document.createElement('div');
    textContainer.id = textContainerId || 'text-container';
    document.body.appendChild(textContainer);

    textData.forEach(({ header, subheader, images, buttons }) => {
        const text = document.createElement('div');
        text.className = 'text';
        text.style.display = 'none';
        text.style.position = 'fixed';
        text.style.top = '75%';
        text.style.left = '50%';
        text.style.transform = 'translate(-50%, -50%)';
        text.style.color = '#fff';
        text.style.fontFamily = 'MyCustomFont', 'Courier New';
        text.style.fontWeight = 'bold';
        text.style.textAlign = 'center';
        text.style.whiteSpace = 'normal';
        text.style.maxWidth = '90%';

        if (header === "Jo-Hann's Hamburger Portfolio") {
            const topImage = document.createElement('img');
            topImage.src = './assets/3d-burger-preview.png';
            topImage.alt = 'Top Image';
        
            // Centering with absolute positioning
            topImage.style.position = 'absolute';
            topImage.style.top = '50%'; 
            topImage.style.left = '50%'; 
            topImage.style.transform = 'translate(-50%, -110%)'; // Perfect centering
            topImage.style.width = 'min(80%, 400px)'; // Responsive sizing
            topImage.style.height = 'auto';
        
            text.appendChild(topImage);
        }
        

        // Create header
        const textHeader = document.createElement('h2');
        textHeader.innerText = header;
        textHeader.style.marginBottom = '10px';
        text.appendChild(textHeader);

        // Create subheader
        const textSubheader = document.createElement('p');
        textSubheader.innerText = subheader;
        textSubheader.style.fontWeight = 'normal';
        text.appendChild(textSubheader);

        // Create container for image-link pairs
        const imageLinkContainer = document.createElement('div');
        imageLinkContainer.style.display = 'flex';
        imageLinkContainer.style.flexDirection = 'row';
        imageLinkContainer.style.alignItems = 'center';
        imageLinkContainer.style.justifyContent = 'center';
        imageLinkContainer.style.gap = '10px';
        imageLinkContainer.style.cursor = 'pointer';
        text.appendChild(imageLinkContainer);

        // Add image-link pairs
        images.forEach(({ src, alt }, index) => {
            const imageLinkWrapper = document.createElement('div');
            imageLinkWrapper.style.display = 'flex';
            imageLinkWrapper.style.flexDirection = 'column';
            imageLinkWrapper.style.alignItems = 'center';
            imageLinkWrapper.style.gap = '10px';
            imageLinkWrapper.style.margin = '5px 0';
            imageLinkWrapper.style.border = '2px solid white';
            imageLinkWrapper.style.padding = '5px';
            imageLinkWrapper.style.borderRadius = '10px';

            // Add image
            const img = document.createElement('img');
            img.src = src;
            img.alt = alt;
            img.style.width = '90px';
            img.style.height = '40px';
            img.style.objectFit = 'cover';
            img.style.borderRadius = '5px';
            imageLinkWrapper.appendChild(img);

            // Add button with action
            if (buttons[index]) {
                const button = document.createElement('button');
                button.innerText = buttons[index].text;
                button.style.color = '#ffcc00';
                button.style.fontWeight = 'bold';
                button.style.fontSize = '1em';
                button.style.backgroundColor = 'transparent';
                button.style.border = 'none';
                button.style.cursor = 'pointer';
                imageLinkWrapper.appendChild(button);
            }

            imageLinkWrapper.onclick = buttons[index]?.action;

            imageLinkContainer.appendChild(imageLinkWrapper);
        });

        textContainer.appendChild(text);
    });
}