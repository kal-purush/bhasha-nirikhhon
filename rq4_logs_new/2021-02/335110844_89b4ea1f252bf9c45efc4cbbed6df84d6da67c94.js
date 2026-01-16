import React from "react"

const Display = (props) => {
    const {cookbooks} = props
    console.log(cookbooks)

    const loaded = () => {
        return(<div style={{textAlign: "center"}}>
            {cookbooks.map((cookbook) => (
                <article>
                    <h1>{cookbook.title}</h1>
                    <h1>{cookbook.yearPublished}</h1>
                    <button onClick={() => {
                        props.selectBook(cookbook)
                        props.history.push("/edit")
                    }}>Edit Cookbook</button>
                    <button onClick={() => {
                        props.deleteCookbook(cookbook)
                    }}>Delete Cookbook</button>
                </article>
            ))}
        </div>)
    }
    const loading = <h1>Loading...</h1>

    return cookbooks.length > 0 ? loaded() : loading;
}

export default Display;