import React, { useContext, useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import Axios from 'axios'
import { useForm } from 'react-hook-form'
import styled from 'styled-components'
import cx from 'classnames'
import { baseUrl, projectUrl } from '../api'
import Button from '../components/Button'
import Inputbox from '../components/Inputbox'
import Mask from '../components/Mask'
import { AppContext } from '../store'

const EditProfile = () => {
  const [state, dispatch] = useContext(AppContext);
  const [type, setType] = useState()
  const [errorMessage, setErrorMessage] = useState('')
  const [done, setDone] = useState(false)
  const { register, handleSubmit, errors, setValue } = useForm();

  const onSubmit = async data => {
    delete data.terms
    const userData = {
      first_name: data.name,
      last_name: state.user.id,
      email: data.email,
      password: data.password,
      role: 5,
      status: 'active'
    }
    data.agent = type === 'agent'
    try {
      const response = await Axios.patch(`${projectUrl}/users/${state.user.userId}`, userData, {headers: { Authorization: `Bearer ${state.user.token}` }})
      const userResponse = await Axios.patch(`${baseUrl}/users/${state.user.id}`, data, {headers: { Authorization: `Bearer ${state.user.token}` }})
      setDone(true)
      const payload = {
        ...data,
        ...state.user,
        ...userResponse.data.data,
        ...response.data.data,
        token: state.user.token,
        logged: true,
        userId: response.data.data.id,
        id: userResponse.data.data.id
      }
      dispatch({type: 'SET_USER', payload})
    } catch (error) {
      setErrorMessage(error.response.data.error.message)
    }
  }

  useEffect(() => {
    if (state.user) {
      setType(state.user.agent ? "agent" : "client")
      for (let key in state.user) {
        if (key !== 'password') setValue(key, state.user[key])
      }
    }
  },[setValue, state.user])


  return (
    <section>
      <Mask />
      <div className="wrapper">
        <h1 className=" font-display text-4xl font-semibold w-2/3 mb-8">
          Editar <span className="text-green">perfil</span>
        </h1>
        {
          done
          ? <p className="text-xl">Perfil Editado com sucesso</p>
          : <form onSubmit={handleSubmit(onSubmit)} >
              <Inputbox
                type="text"
                color="green"
                placeholder="nome"
                name="name"
                error={errors.name}
                register={register({required: true})}
              />
              {errors.name && <ErrorMessage>Este campo é obrigatório</ErrorMessage>}

              <Inputbox
                type="email"
                color="green"
                placeholder="e-mail"
                name="email"
                error={errors.email}
                register={register({required: true})}
              />
              {errors.email && <ErrorMessage>Este campo é obrigatório</ErrorMessage>}

              <Inputbox
                type="tel"
                color="green"
                placeholder="telefone"
                name="phone"
                error={errors.phone}
                register={register({required: true})}
              />
              {errors.phone && <ErrorMessage>Este campo é obrigatório</ErrorMessage>}

              <Inputbox
                type="password"
                color="green"
                placeholder="password"
                name="password"
                error={errors.password}
                register={register({required: true})}
              />
              {errors.password && <ErrorMessage>Este campo é obrigatório</ErrorMessage>}

              <select
                name="apartment"
                ref={register({required: true})}
                className={cx(
                  "border-b bg-transparent w-full py-1 mt-6 text-green08 border-green08",
                  {
                    "text-red border-red": errors.apartment
                  })
                }>
                  {
                    type === 'client'
                    ? <>
                      <option value="">apartamento</option>
                      <option value="A">A</option>
                      <option value="B">B</option>
                      <option value="C">c</option>
                      <option value="D">d</option>
                      <option value="E">E</option>
                      <option value="F">F</option>
                      <option value="G">G</option>
                      <option value="H">H</option>
                    </>
                    : <>
                    <option value="">imobiliária</option>
                    <option value="remax">Remax</option>
                    <option value="century21">Century 21</option>
                    </>
                  }
              </select>
              {errors.apartment && <ErrorMessage>Este campo é obrigatório</ErrorMessage>}

              <Inputbox
                type="password"
                color="green"
                placeholder={type === 'agent' ? "código imobiliária" : "código cliente"}
                name="code"
                error={errors.code}
                register={register({required: true})}
              />
              {errors.code && <ErrorMessage>Este campo é obrigatório</ErrorMessage>}

              <p className="text-red mt-4 text-xs">{errorMessage}</p>
              <Button text="guardar" type="primary" className="mt-10" />
              <Link to="/profile">
                <Button text="voltar" type="primary" className="mt-6" />
              </Link>
            </form>
        }
      </div>
    </section>
  )
}

const ErrorMessage = styled.span`
  color: #fe0a01;
  font-size: 12px;
`

export default EditProfile