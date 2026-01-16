

export type AboutUsSection = {

Id_About_Us: number,
Subtitle_About_Us: string,
MissionTitle_About_Us: string,
MissionDescription_About_Us: string,
VisionTitle_About_Us: string,
VisionDescription_About_Us: string,

}

export type HeroSection = {

Id_Hero: number,
Hero_Title: string,
Subtitle_Hero: string,
Hero_Image_Url: string,
HeroText_Button: string,

}


export type NavbarItem = {

Id_Navbar_It: number, //id de navegacion
Title_Nav: string,
UrlNav: string,
Order_Item_Nav: number,
IsActive: boolean,

}


export type SectionPatchOperation = {

    id: number, //id del item al cual vamos a modificar 
    patch: string, //ruta de la entidad
    section : 'HeroSection' | 'AboutUsSection' | 'RegistrationSection' | 'ServiceSection' | 'SiteSettings' | 'TittleSection' | 'NavbarItem', //seccion a la que pertenece el item
    op: 'add' | 'remove' | 'replace' | 'move' | 'copy' | 'test' , //operacion a realizar
    from?: string, //ruta de la entidad origen
    value: string | boolean | number //valor a modificar
}


export type RegistrationSection = {

Id_Registration: number,
Registration_MoreInfoPrompt: string,
RegistrationText_Button: string,
Registration_SupportMessage : string,
Registration_Image_Url: string,

}


export type ServiceSection = {

Id_ServiceSection: number,
Title_Card_SV: string,
Image_Card_SV_Url: string,
Description_Card_SV: string,
SVText_Button: string,

}


export type SiteSettings = {

Id_SiteSettings: number,
SiteTitle: string,
Icon_HGA_Url: string,

}

export type TittleSection = {

Id_TitleSection: number,
Title_Text_Section: string,
Description_Section: string,

}