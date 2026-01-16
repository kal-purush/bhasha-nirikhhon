import React, { useState } from 'react'

const EditUserForm = props => {
  // пользователя которого собираемся редактировать
  const [user, setUser] = useState(props.currentUser)

  const handleInputChange = event => {
    const { name, value } = event.target

    setUser({ ...user, [name]: value })
  }

  const handleSubmit = event => {
    event.preventDefault()
    if (!user.name || !user.username) return

    props.updateUser(user.id, user)
  }

  return (
    <form onSubmit={handleSubmit}>
      <label>Name</label>
      <input
        type="text"
        name="name"
        value={user.name}
        onChange={handleInputChange}
      />
      <label>Username</label>
      <input
        type="text"
        name="username"
        value={user.username}
        onChange={handleInputChange}
      />
      <button>Update user</button>
      <button
        /* обновляем флаг editing, будет представлен в App позже */
        onClick={() => props.setEditing(false)}
        className="btn btn-secondary"
      >
        Cancel
      </button>
    </form>
  )
}

export { EditUserForm }