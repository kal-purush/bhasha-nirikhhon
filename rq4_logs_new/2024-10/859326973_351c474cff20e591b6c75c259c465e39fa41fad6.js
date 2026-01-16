import './Footer.css';

const Footer = () => {
    return (
        <footer className="footer">
            <div className='logoMidia'>
                <img src='/imagens/fb.png' alt='facebook logo'></img>
                <img src='/imagens/tw.png' alt='facebook logo'></img>
                <img src='/imagens/ig.png' alt='facebook logo'></img>
            </div>

            <div className='logoOrgano'>
                <img src='/imagens/logo.png' alt='logo organo'></img>
            </div>

            <div className='marcaDgua'>
                <h6>Desenvolvido por Alura</h6>
            </div>
        </footer>
    )
}

export default Footer;