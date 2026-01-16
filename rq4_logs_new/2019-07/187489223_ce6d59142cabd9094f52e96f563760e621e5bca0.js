import React from 'react'

class BikeRow extends React.Component {

    render() {

        //let bike = this.props.bike

        const strType = this.props.bike.rentalObjectTypes.join(', ')
                            .replace('pedelec', 'Pedelec')
                            .replace('bike', 'Fahrrad')

        const strAdress = this.props.bike.address.street

        return (
            <tr>
                <td>{strType}</td>
                <td>{strAdress}</td>
            </tr>
        )

    }

}

export default BikeRow