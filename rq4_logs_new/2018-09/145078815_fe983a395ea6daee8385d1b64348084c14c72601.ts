export class Constants {
	API_BASEURL = "http://10.220.28.100:8083/pocwebapp";

	ERR_FIRST_NAME= {
		"required": "First Name required",
		"invalid": "Provide valid first name"
	};

	ERR_LAST_NAME= {
		"required": "Last Name required",
		"invalid": "Provide valid last name"
	};


	QUOTATION_TBL_HEADERS = ["Id" ,"Name", "Contact", "Variant", "Action"];

	DUMMY_DATA = [
				    {
				        "quotation_id": 4,
				        "user": {
				            "id": 37,
				            "ssoId": "Taaaabcx",
				            "password": "$2a$10$tpauAvIF96E7rjPw6zOzRuVSB3nOIqSSvIdNmikKuTsg3AnhEtFeq",
				            "firstName": "Taaaest",
				            "lastName": "abcxyz",
				            "email": "pallavi.b.patil@capegmini.com",
				            "mobileNumber": "998707556",
				            "address": null,
				            "zipcode": null,
				            "state": null,
				            "city": null,
				            "country": null,
				            "userProfiles": [
				                {
				                    "id": 3,
				                    "type": "DBA"
				                }
				            ]
				        },
				        "variant": {
				            "variantId": 5,
				            "variantCode": "V05",
				            "variantName": "Verna EX",
				            "imagePath": "/images/cgmotors/VernaEX.jpg",
				            "price": "750000.0",
				            "categoryName": null
				        },
				        "dealer": {
				            "id": 1,
				            "code": "D01",
				            "name": "Modi Hyundai",
				            "address": "G/02 Vikas Centre,Swami Vivekananda Rd, BEST Colony, Santacruz West, Mumbai, Near Milan Subway",
				            "pincode": "400054",
				            "website": "http://modi.hyundaimotor.in",
				            "email": "salessantacruz@modihyundai.com",
				            "mobileNumber": "9918937988",
				            "landline": "23873929",
				            "latitude": "72.7174229",
				            "longitude": "19.2313187"
				        },
				        "city": {
				            "cityId": 1,
				            "cityCode": "CT01",
				            "cityName": "MUMBAI",
				            "isActive": "Y"
				        },
				        "discountedPrice": 0,
				        "filePath": "0"
				    }
				]


}