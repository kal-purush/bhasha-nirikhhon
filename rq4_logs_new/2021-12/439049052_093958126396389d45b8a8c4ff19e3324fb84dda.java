
class Menu {

    public static void menu() {
        int option = 0;
        while (option != 9) {
            System.out.println(" Press " +
                    " \n1) Add Contact " +
                    " \n2) Edit Address " +
                    " \n3) Edit City " +
                    " \n4) Edit State " +
                    " \n5) Edit Zip " +
                    " \n6) Edit Phone Number " +
                    " \n7) Edit Email-Id " +
                    " \n8) Delete Contact " +
                    " \n9) Quit App " );

            AddressBook obj = new AddressBook();
            ContactData contactDataObject = new ContactData();

            option = obj.scan.nextInt();
            switch (option) {
                case 1:
                    contactDataObject.addContact();
                    System.out.println(ContactData.contactsMap);
                    break;
                case 2:
                    contactDataObject.changeAddress();
                    System.out.println(ContactData.contactsMap);
                    break;
                case 3:
                    contactDataObject.changeCity();
                    System.out.println(ContactData.contactsMap);
                    break;
                case 4:
                    contactDataObject.changeState();
                    System.out.println(ContactData.contactsMap);
                    break;
                case 5:
                    contactDataObject.changeZip();
                    System.out.println(ContactData.contactsMap);
                    break;
                case 6:
                    contactDataObject.changePhoneNumber();
                    System.out.println(ContactData.contactsMap);
                    break;
                case 7:
                    contactDataObject.changeEmail();
                    System.out.println(ContactData.contactsMap);
                    break;
                case 8:
                    contactDataObject.deleteContact();
                    System.out.println(ContactData.contactsMap);
                    break;
                case 9:
                    break;
                default:
                    System.out.println(" Please choose from above numbers ");
            }
        }

    }
}