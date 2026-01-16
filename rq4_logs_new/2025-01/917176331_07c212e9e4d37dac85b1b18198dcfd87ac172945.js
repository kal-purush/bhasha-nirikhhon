import { Container, Row, Col } from 'react-bootstrap'

const TermsAndConditions = () => {
  return (
    <Container className="mt-5">
      <Row>
        <Col>
          <h1>Terms and Conditions</h1>
          <p>
            <strong>Last updated:</strong> [Date]
          </p>

          <h4>1. Acceptance of Terms</h4>
          <p>
            By registering on our website, you agree to comply with these Terms
            and Conditions. If you do not agree, please do not use our site.
          </p>

          <h4>2. Use of the Website</h4>
          <ul>
            <li>The website is provided "as is" for educational purposes.</li>
            <li>You agree to use the site lawfully and responsibly.</li>
            <li>
              Attempting to compromise site security or gain unauthorized access
              to data is prohibited.
            </li>
          </ul>

          <h4>3. User Accounts</h4>
          <p>
            You must provide accurate information during registration. You are
            responsible for maintaining the confidentiality of your login
            credentials.
          </p>

          <h4>4. Limitation of Liability</h4>
          <p>
            We are not liable for any damages resulting from the use of this
            website.
          </p>

          <h4>5. Changes to Terms</h4>
          <p>
            We reserve the right to modify these terms at any time without prior
            notice. Updates will be effective immediately upon posting on the
            website.
          </p>

          <h4>6. Contact Information</h4>
          <p>
            If you have any questions, please contact us at{' '}
            <a href="mailto:your_email@example.com">fedorin.mir@gmail.com</a>.
          </p>
        </Col>
      </Row>
    </Container>
  )
}

export default TermsAndConditions