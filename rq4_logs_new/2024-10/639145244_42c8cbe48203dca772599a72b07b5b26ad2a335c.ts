export interface MainCampos {
    Comuna:           Comuna[];
    Tipo_vehiculo:    TipoVehiculo[];
    Region:           Region[];
    Giro_factura:     GiroFactura[];
    Paises:           Paise[];
    Tipo_carroceria:  Tipo[];
    Tipo_adicionales: Tipo[];
}

export interface Comuna {
    Nombre_comuna: string;
    Id_region:     string;
    Id_comuna:     string;
}

export interface GiroFactura {
    Id:   number;
    Giro: string;
}

export interface Paise {
    Id:           number;
    Pais:         string;
    Cod_telefono: string;
}

export interface Region {
    Id_region:     string;
    Nombre_region: string;
}

export interface Tipo {
    Id:   number;
    Tipo: string;
}

export interface TipoVehiculo {
    Id:   number;
    Name: string;
}