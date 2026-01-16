import { InquiryData } from "../../../src/modules/inquiry";
import InquiryStore from '../inquiry_store';

const inquiry = <InquiryData>{};

export interface InquiryProps {
  request: Request;
} 

const Inquiry : PagesFunction<InquiryProps> = async ({request}) => {
  const inquiries = new InquiryStore();
  const inquiryID = parseInt(request.headers.get('id'));

  const body = JSON.stringify(await inquiries.find(inquiryID));
  const headers = { 'Content-type': 'application/json' };
  return new Response(body, { headers });
};

export default Inquiry;