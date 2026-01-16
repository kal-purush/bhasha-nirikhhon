import React, {Fragment, useState, useEffect} from 'react';
import Header from './components/Header';
import Form from './components/Form';
import ListNews from './components/ListNews';

function App() {

  const [category, setCategory] = useState('');
  const [ newsList, setNewsList] = useState([]);

  useEffect(() => {
    const RequestAPI = async () => {
      const apiKey = '677b6e6273824a5bb9fd17101e681665';
      const url = `https://newsapi.org/v2/top-headlines?country=ve&category=${category}&apiKey=${apiKey}`;
      const resp = await fetch(url);
      const news = await resp.json();
      setNewsList(news.articles);
    }
    RequestAPI()
  }, [category]);

  return (
    <Fragment>
      <Header 
        title="Buscador de Noticias"
      />

      <div className="container white">
        <Form 
          setCategory={setCategory}
        />
        <ListNews 
          newsList={newsList}
        />
      </div>
    </Fragment>
    
  );
}

export default App;