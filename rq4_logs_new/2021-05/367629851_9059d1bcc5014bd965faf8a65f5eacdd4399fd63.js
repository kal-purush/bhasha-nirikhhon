import React, { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { useDispatch, useSelector } from "react-redux";
import PageHeader from '../Shared/PageHeader';

import './Registration.css';
import { IMAGE_URL } from '../../agent';
import { loadRegUsers } from '../../store/modules/userRegister';

const RegisteredUsers = (props) => {
    const dispatch = useDispatch();

    const regUsers = useSelector(state => state.userRegister.registeredUsers);
    const [pageNumber, setPageNumber] = useState(1)
    const [search, setSearch] = useState("");

    console.log({ regUsers })

    const eventId = props.match.params.id;

    useEffect(() => {
        dispatch(loadRegUsers(pageNumber));
    }, [dispatch, pageNumber]);

    const loadmore = () => {
        setPageNumber(pageNumber + 1)
        dispatch(loadRegUsers(pageNumber + 1));
    }


    return (
        <div className="list-poll col-lg-9 col-md-8 col-12">
            <PageHeader pageTitle={'Registered Users'} count={regUsers?.length}>
                <Link to={`/events/view/${eventId}`} className="pl-3">
                    <button type="button" className="btn btn-md project-color size-text mr-2 text-white">
                        <i className="fas fa-arrow-left"></i>&nbsp;Back</button>
                </Link>
            </PageHeader>
            <div className="card mt-1">

                <div className="card-body">

                    <div className="apex-charts row">
                        {regUsers && regUsers.length > 0 && regUsers.map(regUser =>

                            <div className="card mb-1 shadow-lg card-hover zoomCard">
                                {/* {event.coverImage ? <img src={IMAGE_URL + event.coverImage} alt="Cover Image" width="230px" height="150px" /> :
                                    <img src="../assets/images/event.jpg" alt="Cover Image" width="230px" height="150px" />
                                } */}
                                <div className="card-body">
                                    <div className="row no-gutters">
                                        <div className="col-auto">

                                            <small className="text-muted">
                                                <span className="font-weight-bold"> Name: </span> {regUser.name} <br />
                                                <small className="text-muted">
                                                    <span className="font-weight-bold">Email: </span> {regUser.email} <br />
                                                </small>
                                                <small className="text-muted">
                                                    <span className="font-weight-bold">Phone Number : </span>{regUser.phoneNumber} <br />
                                                </small>
                                                <small className="text-muted">
                                                    <span className="font-weight-bold">Access Code : </span>{regUser.accessCode} <br />
                                                </small>
                                            </small>
                                        </div>
                                    </div>

                                </div>
                            </div>
                        )}

                        {
                            regUsers?.length === 0 && <strong className="mx-auto">No event found</strong>
                        }

                        {
                            regUsers?.length >= 25 && <div className="col-xl-12 col-lg-12 col-md-12 col-12 text-center mt-4">
                                <Link className="btn btn-primary">
                                    <div className="spinner-border spinner-border-sm mr-2" role="status">
                                        <span className="sr-only">Loading...</span>
                                    </div>Load more
                                    </Link>
                            </div>
                        }
                    </div>
                </div>
            </div>
        </div>

    )
}

export default RegisteredUsers;