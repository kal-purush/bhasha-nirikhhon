import { CustomModalType, ModalContentProps } from "@youmeet/types/CustomModal";

export const modals: { [type in CustomModalType]: ModalContentProps } = {
  "upload-50": {},
  delete: {},
  fulfill: {},
  "request-feedback": {
    content: {
      fr: "Merci de nous laisser un feedback dans votre dashboard en nous précisant la problème.+",
      en: "We ask you to go to your dashboard and leave us an explanation of the problem, so that we can resolve it as soon as possible.+",
    },
    title: {
      fr: "Vous venez de rencontrer une erreur.+",
      en: "You just met an error.+",
    },
  },
  custom: {},
  shareProfile: {},
  requestNotCompleted: {
    content: {
      fr: "+Votre demande= n'a pas pu être prise en compte. Réessayez plus tard.+",
      en: "+Your request= could not be taken into account. Please try again later.+",
    },
    title: {
      fr: "Désolé.+",
      en: "Sorry.+",
    },
  },
  home: {},
  offer: {},
  profils: {},
  publicOffer: {},
  sharing: {},
  creditTooLow: {
    content: {
      fr: "+Votre crédit= est trop bas. Ajoutez du crédit ou souscrivez à un abonnement.+",
      en: "+Your credit= is too low. Add some credit or subscribe.+",
    },
    title: {
      fr: "Vous souhaitez voir ce profil.+",
      en: "You wish to check up this profile.+",
    },
  },
  interviewOffer: {
    content: {
      fr: "+Votre demande= a bien été prise en compte. Le candidat reçoit une notification.+",
      en: "+Your request= has been taken into account. The candidate receives a notification.+",
    },
    title: {
      fr: "Merci.+",
      en: "Thank you.+",
    },
  },
  record: {},
  remark: {
    content: {
      fr: "Que pensez-vous des fonctionnalités ? Quelle fonctionnalité utilisez-vous le plus ? Y a t-il une fonctionnalité que vous aimeriez ajouter ?.+",
      en: "What do you think of the features ? Which feature do you use the most ? Is there a feature that you wish to be added ?.+",
    },
    title: {
      fr: "Votre opinion sur l'application+",
      en: "Your opinion on the app+",
    },
  },
  "not-completed": {
    content: {
      fr: "L'action n'a pas été complétée.+",
      en: "The action was not completed.+",
    },
    title: {
      fr: "Réessayez plus tard+",
      en: "Try again later+",
    },
  },
  backoffice: {},
  notifications: {},
  loginPage: {},
  login: {},
  retrieval: {},
  consent3: {},
  feedback: {
    content: {
      fr: "Exprimez votre avis sur la présentation.+",
      en: "Tell your feeling about the presentation.+",
    },
    title: {
      fr: "Feedback+",
      en: "Feedback+",
    },
  },
  videoAdding: {
    content: {
      fr: "Ajoutez les informations suivantes:+",
      en: "Add the following documents:+",
    },
    title: {
      fr: "Vous souhaitez postuler.+",
      en: "You wish to apply.+",
    },
    cta: { fr: "Postuler maintenant+", en: "Apply now+" },
  },
  backofficeConfirm: {
    content: {
      fr: "+Votre demande= a bien été prise en compte.+",
      en: "+Your request= has been taken into account.+",
    },
    title: {
      fr: "Merci.+",
      en: "Thank you.+",
    },
  },
  offerConfirm: {
    content: {
      fr: "+Votre ajout= a bien été pris en compte. Les candidats vont pouvoir consulter votre nouvelle offre.+",
      en: "+Your adding= has been taken into account. The candidates will be able to consult your new offer.+",
    },
    title: {
      fr: "Merci.+",
      en: "Thank you.+",
    },
  },
  consent2: {
    content: {
      fr: "+Votre demande= a bien été prise en compte. L'entreprise reçoit une notification.+",
      en: "+Your request= has been taken into account. The company receives a notification.+",
    },
    title: {
      fr: "Merci.+",
      en: "Thank you.+",
    },
  },
  upload: {},
  video: {},
  unauthorized: {
    title: {
      fr: "Une erreur est survenue.+",
      en: "An error occured.+",
    },
    content: {
      fr: "Vous ne pouvez ajouter de vidéo pour le moment, réessayez plus tard.+",
      en: "You can't add a video now, try again later.+",
    },
  },
  fileTooLarge: {
    title: {
      fr: "Votre fichier est trop volumineux !+",
      en: "Your file is too large!+",
    },
    content: {
      fr: "Essayez de compresser, réduire la taille du fichier.+",
      en: "Try to compress, reduce file size.+",
    },
  },
  consent: {
    content: {
      fr: `Un email vous a été renvoyé !
    Nous vous redirigerons vers notre page d'accueil.+`,
      en: `An email has been sent to you!
        We will redirect you to our home page.+`,
    },
  },
  form: {
    title: {
      fr: "Complétez votre profil avant !+",
      en: "Complete your profile before !+",
    },
    content: {
      fr: `Quel plaisir de vous trouver ici !
    D'abord, prenez le temps de compléter votre profil, vous profiterez un maximum de notre service.+`,
      en: `What a pleasure to find you here!
    First, take the time to complete your profile, you will get the most out of our service.+`,
    },
  },
  search: {
    title: {
      fr: "Complétez votre profil avant !+",
      en: "Complete your profile first!+",
    },
    content: {
      fr: `Quel plaisir de vous trouver ici !
    D'abord, prenez le temps de compléter votre profil, vous profiterez un maximum de notre service.+`,
      en: `What a pleasure to find you here!
    First, take the time to complete your profile, you will get the most out of our service.+`,
    },
    cta: { fr: "Créer profil+", en: "Create profile+" },
  },
  account: {
    title: {
      fr: "Vous souhaitez en savoir plus.+",
      en: "You would like to know more.+",
    },
    cta: { fr: "Souscrire+", en: "Subscribe+" },
    content: {
      fr: `Les profils hautement qualifiés sont +à porter de main=. Il vous suffit de choisir votre abonnement.+`,
      en: `Highly qualified profiles are +at your fingertips=. All you have to do is choose your subscription.+`,
    },
  },
  accountCandidate: {
    title: {
      fr: "Vous souhaitez en savoir plus.+",
      en: "You would like to know more.+",
    },
    cta: { fr: "Souscrire+", en: "Subscribe+" },
    content: {
      fr: `Vous souhaitez +obtenir des références= pour les ajouter à votre profil lors de vos candidatures. Parfait, nous pouvons vous aider !+`,
      en: `You would like to +get references= to add to your profile when applying. Perfect, we can help you!+`,
    },
  },
};