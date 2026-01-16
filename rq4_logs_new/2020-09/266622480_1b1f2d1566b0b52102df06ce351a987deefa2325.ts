import { LeadPropertyMetadata } from 'src/models/lead-property-metadata';
import * as LeadPropertyMetadataActions from './lead-property-metadata.actions'

export interface LeadPropertyMetadataState {
    Properties: LeadPropertyMetadata[]
}

const initialState: LeadPropertyMetadataState = {
    Properties: []
}

export function leadPropertyMetadataReducer(state: LeadPropertyMetadataState = initialState, action: LeadPropertyMetadataActions.LeadPropertyMetadataActions): LeadPropertyMetadataState {
    switch (action.type) {
        case LeadPropertyMetadataActions.LEAD_PROPERTY_METADATA_UPDATE:
            return {
                ...state,
                Properties: action.payload
            }
    }
}