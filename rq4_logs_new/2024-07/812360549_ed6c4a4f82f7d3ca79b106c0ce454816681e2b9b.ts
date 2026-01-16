import {NextRequest} from "next/server"
import {Font} from "@/types/font.interfaces"


const FontsDatabase: Font[] = [
    {
        _id: "v_CCMeanwhile",
        family: "v_CCMeanwhile",
        categories: ["Bubble"],
        langs: ["rus"],
        variants: [
            "Regular"
        ],
        files: {
            "Regular": "/fonts/v_CCMeanwhile/v_ccmeanwhile_regular.ttf"
        },
        sizeCoeff: 0.85,
    },
    {
        _id: "CCMeanwhile",
        family: "CCMeanwhile",
        categories: ["Bubble"],
        langs: ["rus"],
        variants: [
            "Regular",
            "Bold",
            "Italic",
            "BoldItalic"
        ],
        files: {
            "Regular": "/fonts/CCMeanwhile/CCMeanwhile-Regular.otf",
            "Bold": "/fonts/CCMeanwhile/CCMeanwhile-Bold.otf",
            "Italic": "/fonts/CCMeanwhile/CCMeanwhile-Italic.otf",
            "BoldItalic": "/fonts/CCMeanwhile/CCMeanwhile-BoldItalic.otf"
        },
        sizeCoeff: 0.85,
    },
    {
        _id: "CCSamaritan",
        family: "CCSamaritan",
        categories: ["Bubble"],
        langs: ["rus"],
        variants: [
            "Regular",
            "Bold",
            "Italic",
            "BoldItalic"
        ],
        files: {
            "Regular": "/fonts/CCSamaritan/CCSamaritan-Regular.otf",
            "Bold": "/fonts/CCSamaritan/CCSamaritan-Bold.otf",
            "Italic": "/fonts/CCSamaritan/CCSamaritan-Italic.otf",
            "BoldItalic": "/fonts/CCSamaritan/CCSamaritan-BoldItalic.otf"
        },
        sizeCoeff: 0.8,
    },
    {
        _id: "CCSamaritanLower",
        family: "CCSamaritanLower",
        categories: ["Bubble"],
        langs: ["rus"],
        variants: [
            "Regular",
            "Bold",
            "Italic",
            "BoldItalic"
        ],
        files: {
            "Regular": "/fonts/CCSamaritanLower/CCSamaritanLower-Regular.otf",
            "Bold": "/fonts/CCSamaritanLower/CCSamaritanLower-Bold.otf",
            "Italic": "/fonts/CCSamaritanLower/CCSamaritanLower-Italic.otf",
            "BoldItalic": "/fonts/CCSamaritanLower/CCSamaritanLower-BoldItalic.otf"
        },
        sizeCoeff: 0.8,
    },
    {
        _id: "CCSamaritanTall",
        family: "CCSamaritanTall",
        categories: ["Bubble"],
        langs: ["rus"],
        variants: [
            "Regular",
            "Bold",
            "Italic",
            "BoldItalic"
        ],
        files: {
            "Regular": "/fonts/CCSamaritanTall/CCSamaritanTall-Regular.otf",
            "Bold": "/fonts/CCSamaritanTall/CCSamaritanTall-Bold.otf",
            "Italic": "/fonts/CCSamaritanTall/CCSamaritanTall-Italic.otf",
            "BoldItalic": "/fonts/CCSamaritanTall/CCSamaritanTall-BoldItalic.otf"
        },
        sizeCoeff: 0.95,
    },
    {
        _id: "CCSamaritanTallLower",
        family: "CCSamaritanTallLower",
        categories: ["Bubble"],
        langs: ["rus"],
        variants: [
            "Regular",
            "Bold",
            "Italic",
            "BoldItalic"
        ],
        files: {
            "Regular": "/fonts/CCSamaritanTallLower/CCSamaritanTallLower-Regular.otf",
            "Bold": "/fonts/CCSamaritanTallLower/CCSamaritanTallLower-Bold.otf",
            "Italic": "/fonts/CCSamaritanTallLower/CCSamaritanTallLower-Italic.otf",
            "BoldItalic": "/fonts/CCSamaritanTallLower/CCSamaritanTallLower-BoldItalic.otf"
        },
        sizeCoeff: 1
    },
]

export function GET(req: NextRequest) {
    return new Response(JSON.stringify(FontsDatabase))
}