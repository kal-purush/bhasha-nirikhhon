import { NextRequest } from 'next/server';
import { supabaseAdmin } from '@/utils/supabase-admin';
import { sendEmail } from '@/utils/email';
import { DatabaseError } from '@/utils/errors';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

export async function POST(req: NextRequest) {
  try {
    const { newsletterId } = await req.json();

    // Get newsletter with company data
    const { data: newsletter, error: newsletterError } = await supabaseAdmin
      .from('newsletters')
      .select(`
        *,
        companies (
          company_name,
          industry,
          target_audience,
          contact_email
        )
      `)
      .eq('id', newsletterId)
      .single();

    if (newsletterError) throw new DatabaseError(`Failed to fetch newsletter: ${newsletterError.message}`);
    if (!newsletter) throw new Error('Newsletter not found');
    if (!newsletter.companies) throw new Error('Company data not found');

    const company = newsletter.companies;

    // Parse the sections
    const section1 = newsletter.section1_content ? JSON.parse(newsletter.section1_content) : null;
    const section2 = newsletter.section2_content ? JSON.parse(newsletter.section2_content) : null;
    const section3 = newsletter.section3_content ? JSON.parse(newsletter.section3_content) : null;

    if (!section1 || !section2 || !section3) {
      throw new Error('Newsletter content not fully generated yet');
    }

    // Create HTML email content
    const emailHtml = `
      <!DOCTYPE html>
      <html>
      <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>${company.company_name} Newsletter Draft</title>
      </head>
      <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333; max-width: 800px; margin: 0 auto; padding: 20px;">
        <div style="text-align: center; padding: 20px 0; border-bottom: 2px solid #eee;">
          <h1 style="color: #2c5282; margin: 0;">${company.company_name} Newsletter</h1>
          <p style="color: #666; font-style: italic;">Industry: ${company.industry}</p>
        </div>

        <div style="margin: 30px 0;">
          <h2 style="color: #2d3748;">Industry Summary</h2>
          <div style="background: #f7fafc; padding: 20px; border-radius: 8px;">
            ${newsletter.industry_summary.split('\n').map((line: string) => 
              `<p style="margin: 10px 0;">${line}</p>`
            ).join('')}
          </div>
        </div>

        <!-- Section 1 -->
        <div style="margin: 40px 0; padding: 20px; border: 1px solid #e2e8f0; border-radius: 8px;">
          <h2 style="color: #2d3748; margin-top: 0;">${section1.title}</h2>
          ${section1.imageUrl ? `
            <div style="margin: 20px 0;">
              <img src="${section1.imageUrl}" alt="${section1.title}" style="max-width: 100%; height: auto; border-radius: 4px;">
            </div>
          ` : ''}
          <div style="white-space: pre-line;">
            ${section1.content}
          </div>
        </div>

        <!-- Section 2 -->
        <div style="margin: 40px 0; padding: 20px; border: 1px solid #e2e8f0; border-radius: 8px;">
          <h2 style="color: #2d3748; margin-top: 0;">${section2.title}</h2>
          ${section2.imageUrl ? `
            <div style="margin: 20px 0;">
              <img src="${section2.imageUrl}" alt="${section2.title}" style="max-width: 100%; height: auto; border-radius: 4px;">
            </div>
          ` : ''}
          <div style="white-space: pre-line;">
            ${section2.content}
          </div>
        </div>

        <!-- Section 3 -->
        <div style="margin: 40px 0; padding: 20px; border: 1px solid #e2e8f0; border-radius: 8px;">
          <h2 style="color: #2d3748; margin-top: 0;">${section3.title}</h2>
          ${section3.imageUrl ? `
            <div style="margin: 20px 0;">
              <img src="${section3.imageUrl}" alt="${section3.title}" style="max-width: 100%; height: auto; border-radius: 4px;">
            </div>
          ` : ''}
          <div style="white-space: pre-line;">
            ${section3.content}
          </div>
        </div>

        <div style="margin-top: 40px; padding-top: 20px; border-top: 2px solid #eee; text-align: center;">
          <p style="color: #666;">
            This is your newsletter draft. Please review the content and let us know if you'd like any changes.
          </p>
        </div>
      </body>
      </html>
    `;

    // Send email
    const emailResponse = await sendEmail(
      company.contact_email,
      `Newsletter Draft - ${company.company_name}`,
      emailHtml
    );

    if (!emailResponse.success) {
      throw new Error(`Failed to send email: ${emailResponse.error}`);
    }

    // Update newsletter status
    const { error: updateError } = await supabaseAdmin
      .from('newsletters')
      .update({
        status: 'sent',
        updated_at: new Date().toISOString()
      })
      .eq('id', newsletterId);

    if (updateError) {
      console.error('Failed to update newsletter status:', updateError);
    }

    return Response.json({
      success: true,
      message: 'Newsletter sent successfully',
      data: {
        email: company.contact_email,
        newsletter_id: newsletterId
      }
    });

  } catch (error) {
    console.error('Failed to send newsletter:', error);
    return Response.json({
      success: false,
      message: error instanceof Error ? error.message : 'Failed to send newsletter',
      error: error instanceof Error ? error.message : 'Unknown error'
    }, { status: 500 });
  }
}