import React, { useContext, useState } from 'react'
import { CategoriasContext } from '../context/CategoriasContext'
import { RecetasContext } from '../context/RecetasContext'

export const Formulario = () => {
	const [busqueda, setBusqueda] = useState({
		nombre: '',
		categoria: '',
	})
	const { nombre, categoria } = busqueda

	const { categorias } = useContext(CategoriasContext)
	const { buscarRecetas, setBuscando, buscando } = useContext(RecetasContext)

	const handleChangeInput = ({ target }) => {
		setBusqueda({ ...busqueda, [target.name]: target.value })
	}

	return (
		<form
			autoComplete='off'
			className='col-12'
			onSubmit={(e) => {
				e.preventDefault()
				buscarRecetas(busqueda)
				setBuscando(true)
				setBusqueda({
					nombre: '',
					categoria: '',
				})
			}}
		>
			<fieldset className='text-center'>
				<legend>Busca bebidas por Categoria o Ingredientes</legend>
			</fieldset>

			{buscando && <p className='alert alert-info'>Procesando Busqueda</p>}

			<div className='row mb-md-5 mt-3'>
				<div className='col-md-4 mb-2 mb-md-0'>
					<input
						type='text'
						className='form-control'
						placeholder='Busca por ingrediente'
						name='nombre'
						value={nombre}
						onChange={handleChangeInput}
					/>
				</div>

				<div className='col-md-4 mb-2 mb-md-0'>
					<select
						name='categoria'
						className='form-control'
						value={categoria}
						onChange={handleChangeInput}
					>
						<option value=''>Seleccione Categoria</option>
						{categorias.map((categoria) => (
							<option key={categoria.strCategory} value={categoria.strCategory}>
								{categoria.strCategory}
							</option>
						))}
					</select>
				</div>

				<div className='col-md-4 mb-2 mb-md-0'>
					<input
						type='submit'
						className='btn btn-success btn-block'
						value='Buscar Bebidas'
					/>
				</div>
			</div>
		</form>
	)
}