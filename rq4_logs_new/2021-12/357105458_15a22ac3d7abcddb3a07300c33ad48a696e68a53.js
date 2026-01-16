import React from 'react'
import { Arrow } from '../../icons'
import { StyledPagination } from './styles'
import PGNButton from './_pgnButton/PGNButton'
import { v4 as uuidv4 } from 'uuid';

const Pagination = () => {

    const renderPagination = (array) => {
        if (array.length < 5) {
            return (
                <>
                    {array.map((el, idx) => (
                        <PGNButton key={uuidv4()}>
                            {idx}
                        </PGNButton>
                    ))}
                </>
            )
        } else if (array.length >= 5) {
            return (
                <>
                    {array.slice(0, 3).map((el, idx) => (
                        <PGNButton key={uuidv4()}>
                            {idx + 1}
                        </PGNButton>
                    ))}
                    <PGNButton styles={{border: '1px solid #fff', cursor: 'initial'}}>...</PGNButton>
                    <PGNButton>{array.length}</PGNButton>
                </>
            )
        }
    }

    return (
        <StyledPagination>
            <PGNButton>
                <Arrow />
            </PGNButton>
            {renderPagination([...new Array(20)])}
            <PGNButton>
                <Arrow type='right'/>
            </PGNButton>

        </StyledPagination>
    )
}

export default Pagination