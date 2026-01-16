import React, {useContext} from 'react'
import { useSearchParams } from 'react-router-dom'
import {ProductsContext} from '../Context/ProductsContext.js'

export function AddUrlQuery(value) {
	const {category, searchstring} = useContext(ProductsContext)

	const [searchParams, setSearchParams] = useSearchParams()

	if (category.trim()===''||category.trim()==='all') {
		setSearchParams({'q':value})
	} else if (searchstring.trim()==='') {
		setSearchParams({'category':value})
	} else {
		setSearchParams({category, 'q':searchstring})
	}
}