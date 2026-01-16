import { useState, useEffect } from 'react'; // Using hooks to determine size for responsiveness
import { ThemeProvider } from 'styled-components';
import GlobalStyles from './components/styles/Global';
import theme from './components/styles/Theme';
import Header from './components/Header';
import Title from './components/Title';
import Search from './components/Search';
import Table from './components/Table';
import { Container } from './components/styles/Container.styles';
import Footer from './components/Footer';

function App() {
  const [windowSize, setWindowSize] = useState(0);
  const [top10, setTop10] = useState([]);
  // Nomics Key
  const key = '7069f6efa90b59ebeecb07454127d5887d88cc2d';

  useEffect(() => {
    const getTop10 = async () => {
      const top10FromServer = await fetchTop10();
      setTop10(top10FromServer);
    };
    getTop10()

    updateWidth();
    window.addEventListener('resize', updateWidth);

    return () => {
      window.removeEventListener('resize', updateWidth);
    }
  }, []);

  // Update window width state
  const updateWidth = () => {
    const width = window.innerWidth;
    setWindowSize(width);
  };

  const responsive = {
    mobile: windowSize < 1023
  };

  

  // Get top 10 cryptos
  const fetchTop10 = async () => {
      const res = await fetch(`https://api.nomics.com/v1/currencies/ticker?key=${key}&interval=1d,30d&per-page=10&page=1`);
      const data = await res.json();

      return data;
  };

  return (
    <ThemeProvider theme={theme}>
      <>
      <GlobalStyles />
        <Container>
          <Header isMobile={responsive.mobile} />
          <Title />
          <Search />
          <Table top10={top10} />
        </Container>
        <Footer isMobile={responsive.mobile} />
      </>
    </ThemeProvider>
  );
}

export default App;