import { Body, Controller, Post, Route, SuccessResponse } from 'tsoa';
import { ContactInputDTO } from '../dto/contact.dto';

@Route('contact')
export class ContactController extends Controller {
  constructor() {
    super();
  }

  /**
   * Send an email
   */
  @Post()
  @SuccessResponse('200', 'Email sent')
  async sendEmail(@Body() requestBody: ContactInputDTO): Promise<void> {
    const result = ContactInputDTO.safeParse(requestBody);

    if (!result.success) {
      this.setStatus(400);
      const zodErrorMessage = result.error.errors[0].message;
      throw new Error(zodErrorMessage);
    }
  }
}