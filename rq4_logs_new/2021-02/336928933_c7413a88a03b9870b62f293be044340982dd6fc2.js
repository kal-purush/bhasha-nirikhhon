import React, { useState } from 'react'

function Search({searchFunc}) {
    const [text, setText] = useState("")

    const textHandler = (e) => {
        setText(e.currentTarget.value)
    }
    const onSubmitText = (e) => {
        e.preventDefault();
        searchFunc(text)
    }

    return (
        <div>
            <form onSubmit={onSubmitText}>
                <input  type="text" placeholder="앨범명을 적어주세요" value={text} onChange={textHandler}/>
                <button>검색</button>
            </form>
        </div>
    )
}

export default Search;