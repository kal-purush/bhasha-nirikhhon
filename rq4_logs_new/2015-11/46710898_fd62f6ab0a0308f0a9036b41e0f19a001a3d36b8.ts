export interface EapTlsModel {

}

export interface EapPeapModel {

}

export interface EapModel {
	tls: EapTlsModel;
	peap: EapPeapModel;
}