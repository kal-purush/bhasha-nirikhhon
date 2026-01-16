import React, { useState, useEffect } from 'react'
import './form.css'
import { serverRequest } from '../../API/request'
import toast from 'react-hot-toast'
import CircularLoadingButton from '../buttons/loading-button'
import M from 'materialize-css'
import translations from '../../i18n'
import { localStorageSecured } from '../../security/localStorage'
import { config } from '../../config/config'
import LocalOfferIcon from '@mui/icons-material/LocalOffer'

const ClubOfferMessageForm = ({ member }) => {

    const lang = localStorage.getItem('lang')

    const [message, setMessage] = useState()
    const [language, setLanguage] = useState('ar')

    const [isSubmitting, setIsSubmitting] = useState(false)
    const [messageError, setMessageError] = useState()

    useEffect(() => {
        M.AutoInit()
    }, [])

    const resetForm = () => {

        setMessage('')
        setMessageError()

        document.querySelector('#offer-message-form').reset()

    }

    const submitForm = (e) => {

        e.preventDefault()

        if(!message) return setMessageError(translations[lang]['Message is required'])

        const newOfferMessage = {
            message,
            languageCode: language
        }

        const requestHeader = {
            params: { lang },
            headers: {
                'x-access-token': localStorageSecured.get('access-token')
            }
        }

        setIsSubmitting(true)
        serverRequest.post(`/offers-messages/members/${member._id}/send`, newOfferMessage, requestHeader)
        .then(response => {
            setIsSubmitting(false)
            resetForm()
            toast.success(response.data.message, { position: 'top-right', duration: config.TOAST_SUCCESS_TIME })
        })
        .catch(error => {
            setIsSubmitting(false)
            toast.error(error.response.data.message, { position: 'top-right', duration: config.TOAST_ERROR_TIME})
        })
    }

    return (
        <div className="modal" id="offer-message-form-modal">
            <div className="container-fluid white form-container">
            <div className="row modal-content" style={{ margin: 0}}>
            <div className="col s12 form-header-container" style={{ marginBottom: '0rem' }}>
                <h3 className="form-header">
                        <span>
                            {translations[lang]['Send Offer Message']}
                        </span>
                        <div className="stat-icon">
                            <span>
                                <LocalOfferIcon />
                            </span>
                        </div>
                    </h3>
                    <div className="divider form-divider"></div>
                </div>
                <form className="row" name="offer-message-form" onSubmit={submitForm} id="offer-message-form" autocomplete="off">
                    <div className="col s12 row">
                        <div className="input-field input-field-container col s12 m10">
                            <textarea className="materialize-textarea"
                            onChange={e => setMessage(e.target.value)}
                            onClick={e => setMessageError()}
                            style={messageError ? { borderBottom: '1px solid #f44336 ', boxShadow: '0 1px 0 0 #f44336 ' } : null } 
                            ></textarea>

                            { messageError ? 
                            <label for="offer-message-message" style={{ color: '#f44336' }}>{messageError}</label> 
                            : 
                            <label for="offer-message-message">{translations[lang]['Message']}*</label>
                            }
                        </div>
                        <div className="input-field input-field-container col s12 m2">
                            <select onChange={e => setLanguage(e.target.value)}>
                                <option value="ar" selected>عربي</option>
                                <option value="en">English</option>
                            </select>
                            <label>{translations[lang]['Choose Language']}</label>
                        </div>    
                    </div>
                    <div className="col s12 m6">
                        <div className="row form-buttons-container" style={{ paddingLeft: '1rem' }}>
                            <div className="col s12 m3">
                            { isSubmitting ? 
                                <div className="center">
                                    <CircularLoadingButton />
                                </div> 
                                :
                                <button type="submit" className="btn blue">
                                 {translations[lang]['SEND']}
                                </button> 
                                }
                            </div>
                            <div className="col s12 m3">
                                <button className="btn grey modal-close" onClick={e => {
                                    resetForm()
                                    e.preventDefault()
                                }}>
                                    {translations[lang]['CLOSE']}
                                </button>
                            </div>
                        </div>
                    </div>
                </form>
            </div>
        </div>
        </div>
    )
}

export default ClubOfferMessageForm