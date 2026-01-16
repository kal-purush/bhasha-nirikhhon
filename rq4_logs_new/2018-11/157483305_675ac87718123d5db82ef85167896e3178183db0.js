import React, { Component } from 'react'

class About extends Component {
  render() {
    return (
      <div className="container">
        <section id="about" className="about-section">
          <h1>Sobre</h1>
          <div className="jumbotron">
            <h1>Tracker Series</h1>
            <p className="lead text-justify">
              Esta é uma aplicação Web que foi desenvolvida com base no conteúdo das vídeo aulas do minicurso de ReactJS, ministrado pelo instrutor
              <b>Túlio Faria</b> do portal <strong>DevPleno</strong>. O Tracker Series, lhe ajuda a gerenciar suas séries favoritas da NetFlix, fazendo com que
              você não se esqueça mais de uma série que você assistiu ou que algum amigo lhe indicou.
            </p>
            <hr />
            <p className="lead text-justify">
              A Aplicação foi desenvolvida utilizando ReactJS e JSON Server. O minicurso de ReactJS é oferecido pelo portal DevPleno, para maiores informações
              acesse o portal DevPleno clicando no botão logo abaixo.
            </p>
            <a className="btn btn-primary btn-lg" href="https://www.devpleno.com" target="_blank" role="button" rel="noopener noreferrer">
              Saiba mais
            </a>
          </div>
          <hr />
          <div className="float-right">
            <h6 className="display-6">Desenvolvido com</h6>
            <img
              src="https://sentry.io/_assets/logos/react-wordmark-b42c3682d2e7d4895ecd0f7071fa861ea33fc597332fba475c71ca5999d86eba.svg"
              width="200"
              alt="react_logo"
            />
          </div>
        </section>
      </div>
    )
  }
}

export default About