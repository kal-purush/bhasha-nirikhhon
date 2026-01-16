# footer.py

import streamlit as st
from streamlit_lottie import st_lottie
from src.Helper import Helper


class Footer:
    @staticmethod
    def show_footer():
        st.markdown(
            """
            <style>
                .footer {
                    padding: 20px;
                    background-color: #333;
                    color: #fff;
                    text-align: center;
                }

                .footer img {
                    width: 50px;  /* Adjust the width as needed */
                }

                .footer a {
                    color: #fff;
                    margin: 0 10px;
                    text-decoration: none;
                }

                .footer hr {
                    border-color: #555;  /* Set the border color for horizontal lines */
                }
            </style>
            """,
            unsafe_allow_html=True
        )
        st.markdown("<hr>", unsafe_allow_html=True)
        image_col, hyperlinks_col = st.columns([1,3])
        with image_col:
            st.image("resources/logo.png", width=200, use_column_width=False)

        with hyperlinks_col:
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.markdown("<p>AI Platform</p>", unsafe_allow_html=True)
                # AI Platform Section
                st.markdown('<a href="/ai-platform/overview">Overview</a> | <a '
                            'href="/ai-platform/features">Features</a> | <a href="/ai-platform/co-pilot">Co-Pilot</a> '
                            '| <a href="/ai-platform/integrations">Integrations</a> | <a '
                            'href="/ai-platform/documentation">Documentation</a> | <a '
                            'href="/ai-platform/security">Security</a>',
                            unsafe_allow_html=True)
            with col2:
                # Use Cases Section
                st.markdown("<p>Use Cases</p>", unsafe_allow_html=True)
                st.markdown(
                    '<a href="/use-cases/generative-ai">Generative AI</a> | <a '
                    'href="/use-cases/market-intelligence">Market Intelligence</a> | <a '
                    'href="/use-cases/analytics-bi">Analytics & BI</a> | <a '
                    'href="/use-cases/automation">Automation</a> | <a href="/use-cases/all-use-cases">All Use '
                    'Cases</a>',
                    unsafe_allow_html=True)

            with col3:
                # Professional Services Section
                st.markdown("<p>Professional Services</p>", unsafe_allow_html=True)
                st.markdown('<a href="/professional-services/overview">Services Overview</a> | <a '
                            'href="/professional-services/ai-copilots">AI-Copilots</a> | <a '
                            'href="/professional-services/data-ai-strategy">Data & AI Strategy</a> | <a '
                            'href="/professional-services/low-code-development">Low Code App Development</a>',
                            unsafe_allow_html=True)
            with col4:
                # Expertise Section
                st.markdown("<p>Expertise</p>", unsafe_allow_html=True)
                st.markdown(
                        '<a href="/expertise/microsoft-azure">Microsoft Azure</a> | <a href="/expertise/aws">Amazon '
                        'Web Services</a> | <a href="/expertise/google-cloud">Google Cloud Project</a> | <a '
                        'href="/expertise/power-bi">Microsoft Power BI</a> | <a '
                        'href="/expertise/streamlit">Streamlit</a> | <a href="/expertise/databricks">Databricks</a> | '
                        '<a href="/expertise/snowflake">Snowflake</a> | <a href="/expertise/all-expertise">See all</a>',
                        unsafe_allow_html=True)

            with col1:
                # Industries Section
                st.markdown("<p>Industries</p>", unsafe_allow_html=True)
                st.markdown(
                    '<a href="/industries/retail-fmcg">Retail & FMCG</a> | <a '
                    'href="/industries/banking-finance">Banking & Finance</a> | <a '
                    'href="/industries/healthcare">Healthcare</a> | <a href="/industries/other-industries">Other '
                    'Industries</a>',
                    unsafe_allow_html=True)

            with col2:
                # Training & Events Section
                st.markdown("<p>Training & Events</p>", unsafe_allow_html=True)
                st.markdown(
                    '<a href="/training-events/private-training">Private Training</a> | <a '
                    'href="/training-events/user-groups">User Groups</a> | <a '
                    'href="/training-events/events">Events</a>',
                    unsafe_allow_html=True)

            with col3:
                # Resources Section
                st.markdown("<p>Resources</p>", unsafe_allow_html=True)
                st.markdown(
                    '<a href="/resources/case-studies">Case Studies</a> | <a href="/resources/updates">Updates</a> | '
                    '<a href="/resources/news-blogs">News & Blogs</a>',
                    unsafe_allow_html=True)

            with col4:
                # Company Section
                st.markdown("<p>Company</p>", unsafe_allow_html=True)
                st.markdown(
                    '<a href="/company/meet-the-team">Meet the Team</a> | <a href="/company/careers">Careers</a> | <a '
                    'href="/company/contact-us">Contact Us</a> | <a href="/company/support">Support</a>',
                    unsafe_allow_html=True)

        # Use Font Awesome icons for social media
        st.markdown(
            '<script src="https://kit.fontawesome.com/your-fontawesome-kit.js" crossorigin="anonymous"></script>',
            unsafe_allow_html=True)

        st.markdown(
            """
            <div style="display: flex; justify-content: center;">
                <a href="https://twitter.com/yourcompany" target="_blank"><img src="https://cdn4.iconfinder.com/data/icons/social-media-black-white-2/1227/X-1024.png" alt="Twitter" width="30"></a>
                <span style="margin: 0 10px;">|</span>
                <a href="https://linkedin.com/company/yourcompany" target="_blank"><img src="https://cdn3.iconfinder.com/data/icons/free-social-media-22/32/linkedin_social_media_logo-1024.png" alt="LinkedIn" width="30"></a>
                <span style="margin: 0 10px;">|</span>
                <a href="https://instagram.com/yourcompany" target="_blank"><img src="https://cdn2.iconfinder.com/data/icons/social-icons-33/128/Instagram-1024.png" alt="Instagram" width="30"></a>
            </div>
            """,
            unsafe_allow_html=True
        )



        # # Social Media Icons
        # twitter_x = "resources/twitter_X.png"
        # instagram = "resources/instagram.png"
        # linked_in = "resources/linked_in.png"
        # image_paths = [twitter_x, instagram, linked_in]
        #
        # Helper.display_images_side_by_side(image_paths)

        st.markdown("<hr>", unsafe_allow_html=True)
        # Center-align the copyright information directly in the markdown
        st.markdown("<p style='text-align: center;'>&copy; 2024 Data Processor AI. All rights reserved.</p>",
                    unsafe_allow_html=True)



        # Provide links and content for "Terms of Use" and "Privacy Policy"
        st.markdown(
            "<p style='text-align: center;'><a href='/terms-of-use'>Terms of Use</a> | <a "
            "href='/privacy-policy'>Privacy Policy</a></p>",
            unsafe_allow_html=True)