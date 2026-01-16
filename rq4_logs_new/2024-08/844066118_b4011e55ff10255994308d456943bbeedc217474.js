{\rtf1\ansi\ansicpg1252\cocoartf2757
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;}
{\*\expandedcolortbl;;}
\paperw11900\paperh16840\margl1440\margr1440\vieww11520\viewh8400\viewkind0
\pard\tx720\tx1440\tx2160\tx2880\tx3600\tx4320\tx5040\tx5760\tx6480\tx7200\tx7920\tx8640\pardirnatural\partightenfactor0

\f0\fs24 \cf0 import React, \{ useState \} from 'react';\
import axios from 'axios';\
\
const App = () => \{\
  const [urls, setUrls] = useState(['', '', '']);\
  const [metadata, setMetadata] = useState([]);\
  const [error, setError] = useState(null);\
\
  const handleInputChange = (index, value) => \{\
    const newUrls = [...urls];\
    newUrls[index] = value;\
    setUrls(newUrls);\
  \};\
\
  const handleSubmit = async () => \{\
    try \{\
      const response = await axios.post('http://localhost:5000/fetch-metadata', \{ urls \});\
      setMetadata(response.data);\
      setError(null);\
    \} catch (err) \{\
      setError('Failed to fetch metadata. Please check the URLs and try again.');\
      setMetadata([]);\
    \}\
  \};\
\
  return (\
    <div>\
      <h1>URL Metadata Fetcher</h1>\
      \{urls.map((url, index) => (\
        <input\
          key=\{index\}\
          type="text"\
          value=\{url\}\
          onChange=\{(e) => handleInputChange(index, e.target.value)\}\
          placeholder="Enter URL"\
        />\
      ))\}\
      <button onClick=\{handleSubmit\}>Submit</button>\
      \{error && <p>\{error\}</p>\}\
      <div>\
        \{metadata.map((data, index) => (\
          <div key=\{index\}>\
            <h2>\{data.title\}</h2>\
            <p>\{data.description\}</p>\
            <img src=\{data.image\} alt="URL preview" />\
          </div>\
        ))\}\
      </div>\
    </div>\
  );\
\};\
\
export default App;\
}