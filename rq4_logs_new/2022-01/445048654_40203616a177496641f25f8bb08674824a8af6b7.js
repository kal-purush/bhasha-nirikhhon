import styles from '../styles/Global.module.scss';
import {
  Box,
  IconIoLogoFacebook,
  IconIoLogoInstagram,
  IconIoLogoYoutube,
  IconIoLogoWhatsapp,
  IconIoLogoTelegram,
  IconIoLogoTwitter,
  IconIoLogoGithub,
  IconIoMdMail,
  IconIoMdLinkedIn,
} from '../shared/chakra';
import TimelineELement from '../components/TimelineElement';

export default function About(props) {
  const { fullScreen } = props;
  return (
    <>
      <Box
        className={styles.about + (fullScreen ? ' ' + styles.fullScreen : '')}
      >
        <Box className={styles.timelinesContainer}>
          <TimelineELement
            skills={[
              'Commitment',
              'Passion',
              'Team work',
              'Technical knowledge',
            ]}
            date='Feb 2013 - Jul 2019'
            description='Best graduated of the period 2019.1. Final score: 4.375/5'
            title='🎓 ESPE (Escuela Superior Politécnica del Ejército)'
          ></TimelineELement>

          <TimelineELement
            skills={['Ladder', 'C++', 'Java', 'Python', 'MATLAB', 'Wordpress']}
            date='Feb 2014 - Jul 2018'
            description='Software developer, specialist in mechatronics.'
            title='💼 Ten Tech'
          ></TimelineELement>

          <TimelineELement
            skills={[
              'PMP',
              'Leadership',
              'Bussiness administration',
              'Teaching',
            ]}
            date='Feb 2019 - Current'
            description='CEO and developer of the company.'
            title='💼 Localhost (entrepreneurship)'
          ></TimelineELement>

          <TimelineELement
            skills={[
              'Node.js',
              'Flask',
              'Firebase',
              'Linode',
              'Nginx',
              'Certbot',
              'Wordpress',
              'MongoDB',
              'Redis',
              'MQL4 - MQL5',
            ]}
            date='Jan 2020 - Current'
            description='Full stack developer, specialist in Backend and DevOps.'
            title='💼 Kings of Binary'
          ></TimelineELement>

          <TimelineELement
            skills={[
              'AngularJS',
              'Angular',
              'Django',
              'Celery',
              'RabbitMQ',
              'AWS',
              'Firebase',
            ]}
            date='Oct 2020 - Current'
            description='Full stack developer, specialist in Backend and DevOps.'
            title='💼 Devsu'
          ></TimelineELement>
        </Box>
        <Box className={styles.socialNetworks}>
          <div
            onClick={(e) =>
              window.open('https://www.facebook.com/alexandrotapiaflores/')
            }
          >
            <IconIoLogoFacebook></IconIoLogoFacebook>
          </div>
          <div
            onClick={(e) =>
              window.open('https://www.instagram.com/alexandro591/')
            }
          >
            <IconIoLogoInstagram></IconIoLogoInstagram>
          </div>
          <div
            onClick={(e) =>
              window.open(
                'https://www.youtube.com/channel/UCyNec1fTVui8N2p0nKfd9Gw'
              )
            }
          >
            <IconIoLogoYoutube></IconIoLogoYoutube>
          </div>
          <div
            onClick={(e) =>
              window.open('https://web.whatsapp.com/send?phone=593998775709')
            }
          >
            <IconIoLogoWhatsapp></IconIoLogoWhatsapp>
          </div>
          <div onClick={(e) => window.open('https://t.me/alexandrotapia')}>
            <IconIoLogoTelegram></IconIoLogoTelegram>
          </div>
          <div
            onClick={(e) => window.open('https://twitter.com/aatapiaflores')}
          >
            <IconIoLogoTwitter></IconIoLogoTwitter>
          </div>
          <div onClick={(e) => window.open('https://github.com/alexandro591')}>
            <IconIoLogoGithub></IconIoLogoGithub>
          </div>
          <div
            onClick={(e) =>
              window.open(
                'mailto:alexandrotapiaflores@gmail.com?subject=Contact from alexandro.vercel.app'
              )
            }
          >
            <IconIoMdMail></IconIoMdMail>
          </div>
          <div
            onClick={(e) =>
              window.open('https://www.linkedin.com/in/alexandro591/')
            }
          >
            <IconIoMdLinkedIn></IconIoMdLinkedIn>
          </div>
        </Box>
      </Box>
    </>
  );
}