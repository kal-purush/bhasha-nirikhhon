import React, { Component } from 'react';
import { css } from 'glamor';
import glamorous from 'glamorous';
import { Row, Col } from 'glamorous-grid';

const Paragraph = glamorous.p({
	fontSize: 18,
	lineHeight: 'normal',
});

const Heading = glamorous.h2({
	fontSize: 22,
	textAlign: 'center',
	marginTop: 0,
	marginBottom: 20,
});

const Div = glamorous.div({
	marginTop: 10,
});

const style = {
	Container: {
		marginTop: 20,
		marginBottom: 30,
	},
	Hyperlink: {
		display: 'inline-block',
		textDecoration: 'none',
	},
	Image: {
		maxWidth: '100%',
		maxHeight: '100%',
		objectFit: 'contain',
		marginBottom: 10,
	},
};

class ClassicalPiano extends Component {
	render() {
		return (
			<React.Fragment>
				<Row justifyContent="center">
					<Col>
						<Heading>Classical Piano</Heading>
					</Col>
				</Row>
				<Row justifyContent="between">
					<Col span={{ lg: 6 / 12 }}>
						<Div>
							<img
								src="../assets/kjh-orchestra.jpg"
								style={style.Image}
							/>
						</Div>
						<Div>
							<iframe
								width="100%"
								height="166"
								scrolling="yes"
								frameborder="no"
								allow="autoplay"
								src="https://w.soundcloud.com/player/?url=https%3A//api.soundcloud.com/tracks/490609368%3Fsecret_token%3Ds-Y5dVR&color=%23ff5500&auto_play=false&hide_related=false&show_comments=true&show_user=true&show_reposts=false&show_teaser=true"
							/>
						</Div>
						<Div>
							<iframe
								width="100%"
								height="166"
								scrolling="yes"
								frameborder="no"
								allow="autoplay"
								src="https://w.soundcloud.com/player/?url=https%3A//api.soundcloud.com/tracks/490627803%3Fsecret_token%3Ds-zjqeR&color=%23ff5500&auto_play=false&hide_related=false&show_comments=true&show_user=true&show_reposts=false&show_teaser=true"
							/>
						</Div>
					</Col>
					<Col span={{ lg: 6 / 12 }}>
						<div>
							<Paragraph>
								Kimberly Hou began piano at 4 years old with
								Marjorie Lee and has since developed a lifelong
								love for music. A graduate of the Columbia
								University-Juilliard School Exchange Program,
								Kimberly has studied with Matti Raekallio and
								Hung-Kuan Chen. She was named a 2012 U.S.
								Presidential Scholar in the Arts as well as a
								2012 NFAA YoungArts Silver Award Winner. She won
								First Prize in the 2011 Lee University
								International Piano Competition, the 2009 Music
								Teachers National Association Junior Piano
								Competition, the 23rd, 25th, and 27th
								International Young Artist Piano Competitions,
								the Connecticut International Piano Competition,
								and over 25 other regional and national
								competitions. Kimberly is also the 2nd Prize
								Winner of the 2012 William Kapell Young Artist
								Piano Competition, and a winner of the 2010
								Chopin Scholarship by the Chopin Foundation of
								the United States.
							</Paragraph>
							<Paragraph>
								She has been chosen to participate in several
								additional international competitions including
								the 2008 Gina Bachauer International Young
								Artists Piano Competition, the 2009
								Liszt-Garrison International Piano Competition
								(Finalist and Honorable Mention), the 2011
								Virginia Waring International Piano Competition
								(Semifinalist and Honorable Mention), and the
								2011 Minnesota International e-Piano Junior
								Competition (advanced to final 12) among others.
							</Paragraph>
							<Paragraph>
								Kimberly has appeared on NPR’s From the Top as
								well as Richmond CBS Virginia This Morning for a
								live TV interview. She has performed at Carnegie
								Hall’s Weill Hall; Kennedy Center Concert Hall,
								Millennium Stage, and Terrace Theatre; U.S.
								Department of State; Hungarian and Bulgarian
								Embassies; the 2nd Washington International
								Piano Festival; the 2012 William Kapell
								International Piano Festival; the 2011 Kids In
								Need Foundation Annual Gala; and the 2009
								National Conference on Keyboard Pedagogy. In
								addition, she performed in concerts with the
								Richmond, New River Valley, and VCU Symphony
								Orchestras as the winner of concerto
								competitions.
							</Paragraph>
						</div>
					</Col>
				</Row>
			</React.Fragment>
		);
	}
}

export default ClassicalPiano;