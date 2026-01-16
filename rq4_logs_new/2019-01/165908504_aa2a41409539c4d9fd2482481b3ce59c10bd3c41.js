import React from 'react';
import Barcode from 'react-barcode';
import { Col, Row } from 'reactstrap';

export default class BarcodeList extends React.Component {
    render() {
        const { displayValue, codes, displayAddonText, addonText } = this.props;

        return (
            <div>
                {displayAddonText && 
                    addonText.split('\n').map((t, key) => {
                        return (
                            <div key={key}>{t}</div> 
                        ) 
                    })
                }
                <Row>        
                    {codes.map((code, key) => {
                        return (
                            <Col key={key} align="center">
                                <Barcode displayValue={displayValue} value={code} />
                            </Col>
                        )
                    })}
                </Row>
            </div>
        )
    }
}