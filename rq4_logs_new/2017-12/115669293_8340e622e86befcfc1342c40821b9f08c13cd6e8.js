import Document, { Head, Main, NextScript } from 'next/document'
import flush from 'styled-jsx/server'

export default class MyDocument extends Document {
  static getInitialProps ({ renderPage }) {
    const { html, head, errorHtml, chunks } = renderPage()
    const styles = flush()
    return { html, head, errorHtml, chunks, styles }
  }

  render () {
    return (
      <html>
        <Head>
          <link href='https://fonts.googleapis.com/css?family=Roboto' rel='stylesheet' />
          <link rel='stylesheet' href='https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0-beta.2/css/bootstrap.min.css' integrity='sha384-PsH8R72JQ3SOdhVi3uxftmaW6Vc51MKb0q5P2rRUpPvrszuE4W1povHYgTpBfshb' crossOrigin='anonymous' />
          <style>{`body { margin: 0; font-family: Roboto, sans-serif } `}</style>
        </Head>
        <body className='custom_class'>
          {this.props.customValue}
          <Main />
          <NextScript />
        </body>
      </html>
    )
  }
}