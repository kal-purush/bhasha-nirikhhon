/* Import Dependencies */
import axios from 'axios';
import moment from 'moment';

/* Import Types */
import { TaxonomicService, CordraResult, Dict } from 'app/Types';


/**
 * Function that sends a POST request to the API in order to insert a new taxonomic service
 * @param taxonomicService The taxonomic service to insert
 * @returns An instance of Taxonomic Service or undefined
 */
const InsertTaxonomicService = async ({ taxonomicServiceRecord }: { taxonomicServiceRecord?: Dict }) => {
    let taxonomicService: TaxonomicService | undefined;

    if (taxonomicServiceRecord) {
        /* Craft taxonomic service object */
        const newTaxonomicService = {
            type: 'TaxonomicService',
            attributes: {
                content: {
                    taxonomicService: {
                        "@type": 'TaxonomicService',
                        "schema:status": 'proposed',
                        "schema:ratingValue": 0,
                        ...taxonomicServiceRecord
                    }
                }
            }
        };

        try {
            const result = await axios({
                method: 'post',
                url: '/Op.Create',
                params: {
                    targetId: 'service'
                },
                data: newTaxonomicService,
                headers: {
                    'Content-type': 'application/json',
                    // 'Authorization': `Basic ${KeycloakService.GetToken()}`
                },
                auth: {
                    username: 'TaxonomicMarketplace',
                    password: 'mJ07O0VHH8Gv'
                },
                responseType: 'json'
            });

            /* Get result data from JSON */
            const data: CordraResult = result.data;

            /* Set Taxonomic Service */
            taxonomicService = data.attributes.content as TaxonomicService;

            /* Set created and modified */
            taxonomicService.taxonomicService['schema:dateCreated'] = moment(new Date(data.attributes.metadata.createdOn)).format('YYYY-MM-DDTHH:mm:ss.sssZ');
            taxonomicService.taxonomicService['schema:dateModified'] = moment(new Date(data.attributes.metadata.modifiedOn)).format('YYYY-MM-DDTHH:mm:ss.sssZ');
        } catch (error) {
            console.error(error);

            throw (error);
        };
    };

    return taxonomicService;
}

export default InsertTaxonomicService;