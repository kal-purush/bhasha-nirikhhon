import React from "react";
import { graphql } from "gatsby";
import Layout from '../components/Layout';
import Figure from '../components/Figure';
import './contact.css';

const HomePage = ({ data }) => {

	const contact = data.sanityContact;

	return (
		<Layout>
			<section className="flex flex-around contact-layout">
				<article className="card card-transparent-stable contact">
					<h1>Contact Information</h1>
					<p>Chris Hughes</p>
					<p>Email: {contact.email}</p>
					<p>Phone: {contact.phone}</p>
					<ul>
						<li>
							<a href={contact.linkIn}>LinkedIn</a>
						</li>
						<li>
							<a href={contact.Github}>Github</a>
						</li>
					</ul>
				<figure className="contact-profile">
					<Figure id={contact.profile.asset._id} />
				</figure>
				</article>
			</section>
		</Layout>
	);
};

export const query = graphql`
query {
  sanityContact {
    email
    linkIn
    phone
    profile {
      asset {
        _id
      }
    }
    Github
  }
}
`;


export default HomePage;