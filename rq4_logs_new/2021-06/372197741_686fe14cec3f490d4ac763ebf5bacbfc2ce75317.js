/* eslint-disable jsx-a11y/anchor-is-valid */
/* eslint-disable jsx-a11y/label-has-associated-control */
/* eslint-disable import/named */
/* eslint-disable react/button-has-type */
/* eslint-disable react/no-unescaped-entities */
import React, { useContext, useState } from 'react'
import { Link } from 'react-router-dom'
import Modal from '../../components/Modal'
import { UserPosts } from '../../Context/userpostContext'

function Profile() {
    const {
        articles,
        deleteitem,
        setTitle,
        title,
        setBody,
        body,
        Addarticle,
        show,
        setShow,
        status,
        Addstatus,
        user,
    } = useContext(UserPosts)
    const [count, setCount] = useState(12) // initial count to show initial items

    const addMore = () => {
        // function that will make count add by 2 to show 2 more items
        setCount(count + 10)
    }

    const userdetails = (
        <nav className="nav flex-column user-details">
            <a className="nav-link " aria-current="page" href="#">
                <i className="fas fa-envelope-open-text" />
                <span>{user.email}</span>
            </a>
            <a className="nav-link " aria-current="page" href="#">
                <i className="fas fa-phone" />
                <span>{user.phone}</span>
            </a>
            <a className="nav-link " aria-current="page" href="#">
                <i className="fas fa-browser" />
                <span>{user.website}</span>
            </a>
            <a className="nav-link" href="#">
                <i className="fas fa-address-card" />
                <span>{user.address?.street}</span>
            </a>
            <a className="nav-link" href="#">
                <i className="fas fa-city" />
                <span>{user.address?.city}</span>
            </a>
        </nav>
    )

    const articlesList = articles.slice(0, count).map((article) => (
        <div key={article.id} className="col-md-4 mb-2">
            <div className="card">
                <div className="card-body">
                    <h5 className="card-title"> {article.title}</h5>
                    <p className="card-text">{article.body.slice(0, 100)}</p>

                    <Link
                        to={`/own-post/${article.id}`}
                        className="btn theme-outline-btn btn-right"
                    >
                        Read More
                    </Link>
                    <Link
                        to={`/own-post/update/${article.id}`}
                        className="btn btn-outline-success  btn-right"
                    >
                        Update
                    </Link>
                    <button
                        onClick={() => deleteitem(article.id)}
                        className="btn btn-outline-danger  btn-right"
                    >
                        Delete
                    </button>
                </div>
            </div>
        </div>
    ))

    return (
        <section className="articlesList-section">
            <Modal show={show} onClose={() => setShow(false)}>
                <div className="content">
                    <h3>New Post Add</h3>

                    <div className="mb-3">
                        <label className="form-label">Post title</label>
                        <input
                            value={title}
                            type="text"
                            onChange={(e) => setTitle(e.target.value)}
                            className="form-control"
                            placeholder=" Post title"
                        />
                    </div>
                    <div className="mb-3">
                        <label className="form-label">Description</label>
                        <textarea
                            value={body}
                            onChange={(e) => setBody(e.target.value)}
                            className="form-control"
                            rows="5"
                        />
                    </div>
                    <button className="btn theme-btn" onClick={() => Addarticle()}>
                        Save
                    </button>
                    <button className="btn " onClick={() => setShow(false)}>
                        Close
                    </button>
                </div>
            </Modal>
            <div className="container">
                <div className="post_add">
                    <div
                        className={`${status === 201 ? 'alert alert-success' : 'hide'}`}
                        role="alert"
                    >
                        Successfully Add New Item
                        <button className="ml-4 btn btn-danger" onClick={() => Addstatus('')}>
                            Close
                        </button>
                    </div>
                    <div
                        className={`${status === 200 ? 'alert alert-danger' : 'hide'}`}
                        role="alert"
                    >
                        Successfully Delete Item
                        <button className="ml-4 btn btn-danger" onClick={() => Addstatus('')}>
                            Close
                        </button>
                    </div>
                    <button className="btn theme-btn" onClick={() => setShow(true)}>
                        Add New Post
                    </button>
                </div>
                <div className="row">
                    <div className="col-md-3">
                        <div className="profile-sidebar">
                            <div className="profile-userpic">
                                <img
                                    src="https://www.w3schools.com/w3images/avatar2.png"
                                    className="user-img rounded-circle mr-2"
                                    alt=""
                                    width="70"
                                />
                            </div>

                            <div className="profile-usertitle">
                                <div className="profile-usertitle-name">{user.name}</div>
                            </div>
                            <div className="profile-userbuttons">
                                <button type="button" className="btn btn-success btn-sm mr-2">
                                    About
                                </button>
                                <button type="button" className="btn btn-danger btn-sm">
                                    Edit Profile
                                </button>
                            </div>
                            <div className="profile-usermenu">{userdetails}</div>
                        </div>
                    </div>
                    <div className="col-md-9">
                        <div className="row">{articlesList}</div>
                        <div className="load-more">
                            <button className="btn theme-outline-btn mt-4 btn-lg" onClick={addMore}>
                                load more
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        </section>
    )
}

export default Profile