import React from 'react';
import { Query } from 'react-apollo';
import gql from 'graphql-tag';

export class Users extends React.Component {
    render() {
        return <div className="user-container" >
            <Query
                query={gql`{
                allUsers{
                      name
                      id
                      email
                      username
                    }
                }`}
            >
                {({ loading, error, data }) => {
                    if (loading) return <h1>Loading...</h1>
                    if (error) return <h1>Error</h1>
                    return data.allUsers.map((d) => {
                        return <div className="user-card" >
                            <div className="user-image" >
                                <img src={`https://avatars.dicebear.com/v2/avataaars/${d.username}.svg?options[mood][]=happy`} />
                            </div>
                            <div className="user-details">
                                <p> {d.id}</p>
                                <p> {d.name}</p>
                                <p> {d.email}</p>
                            </div>
                        </div>
                    })
                }}
            </Query>
        </div>
    }
}