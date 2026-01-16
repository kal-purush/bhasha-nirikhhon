import React from 'react';
import { ContactInfoTag } from './ContactInfoStyles';
import ContactInfoItem from './ContactInfoItem/ContactInfoItem';
import PhoneIcon from '../../../../assets/icon-phone.svg';
import EmailIcon from '../../../../assets/icon-email.svg';

const ContactInfo = () => {
  return (
    <ContactInfoTag>
      <ContactInfoItem
        iconUrl={PhoneIcon}
        text="Phone: +1-543-123-4567"
        alt="phone icon"
      />
      <ContactInfoItem
        iconUrl={EmailIcon}
        text="example@fylo.com"
        alt="email icon"
      />
    </ContactInfoTag>
  );
};

export default ContactInfo;