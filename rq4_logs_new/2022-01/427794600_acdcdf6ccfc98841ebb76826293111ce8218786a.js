    html += '<div class="profile-top">';
       html += '<div class="pro-card">';
        html += '<div class="profile-pic">';
        if (value.Photo) {
        html += '<img src="' + value.Photo +'">';
        }
        html += '</div>';
        html += '<div class="pro-details">';
         html += '<div class="pro-name">';
          html += '<h1 class="pro-title">'+ value["First Name"] + ' '+ value["Last Name"] +'</h1>';
         html += '</div>';
         html += '<div class="pro-sub">';
          html += '<h2 class="pro-type">'+ value["Type of Pro"] + '</h2>';
          if (value.Firm) {
          html += '<h2 class="pro-firm">'+ ' &#64 ' + value.Firm + '</h2>';
          }
         html += '</div>';
         if (value["Area of Law"]) {
         html += '<div class="pro-block">';
          html += '<h2 class="pro-detail-heading">'+ 'Practice Areas' + '&#58' + '</h2>';
          html += '<p class="pro-detail-text">' +  value["Area of Law"] + '</p>';
         html += '</div>';
         }
         html += '<div class="pro-block">';
          html += '<h2 class="pro-detail-heading">'+ 'Languages' + '&#58' + '</h2>';
          html += '<p class="pro-detail-text">' +  value.Language + '</p>';
         html += '</div>';
         if ((value["Hourly Rate"]) || (value.Contingency) || (value.Consult)) {
         html += '<div class="rates-block">';
           html += '<h2 class="pro-detail-heading">'+ 'Rates' + '&#58' + '</h2>';
           html += '<div class="rates-wrap">';
         }
            if ((value["Hourly Rate"]) || (value.Contingency)) {
            html += '<div class="rates-detail-wrap">';
            }
            if (value["Hourly Rate"]) {
            html += '<p class="pro-detail-text">'+ '&#36' + Number(value["Hourly Rate"]) + '&#46' + Number('  ') + '&#47' + 'hour' + '</p>';
            }
            if (value.Contingency) {
            html += '<p class="pro-detail-text">'+ 'Works on Contingency' + '&#63' + '&#58 ' + value.Contingency + '</p>';
            }
            html += '</div>';
            if (value.Consult) {
            html += '<div class="consult-block">';
            html += '<p class="pro-detail-text">'+ 'Free Consultation' + '&#63' + '&#58 ' + value.Consult + '</p>';
            html += '</div>';
            }
          html += '</div>';
        html += '</div>';
        html += '<div class="contact-buttons space">';
        html += '<button id="contactMe" type="button" class="button Faq left">contact me</button>';
        html += '<button id="seeMore" type="button" class="button Faq right">see more</button>';
        html += '</div>';
       html += '</div>';
       html += '</div>';
      html += '</div>';
      
      html += '<div class="profile-details">';
        if (value.Bio) {
        html += '<div class="bio-block">';
         html += '<h3 class="bio-heading">' + 'Bio' + '&#58' + '</h3>';
         html += '<div class="bio-text">';
          html += '<p class="pro-detail-text">'+ value.Bio + '</p>';
         html += '</div>';
        html += '</div>';
        }
        if (value.Education) {
        html += '<div class="bio-block">';
         html += '<h3 class="bio-heading">' + 'Education' + '&#58' + '</h3>';
         html += '<div class="bio-block-bg">';
      
         html += '</div>';
        html += '</div>';
        }
        if (value.Associations) {
        html += '<div class="bio-block">';
         html += '<h3 class="bio-heading">' + 'Professional Associations ' + '&#38' + ' Memberships' + '&#58' + '</h3>';
         html += '<div class="bio-block-bg">';
      
         html += '</div>';
        html += '</div>';
        }
        if (value.Recognitions) {
        html += '<div class="bio-block">';
         html += '<h3 class="bio-heading">' + 'Recognitions' + '&#58' + '</h3>';
         html += '<div class="bio-block-bg">';
      
         html += '</div>';
        html += '</div>';
        }
        if (value.Publications) {
        html += '<div class="bio-block">';
         html += '<h3 class="bio-heading">' + 'Publications' + '&#58' + '</h3>';
         html += '<div class="bio-block-bg">';
      
         html += '</div>';
        html += '</div>';
        }
        if ((value["Public Phone"]) || (value["Public Email"]) || (value.Url)) {
        html += '<div class="bio-block outline">';
         html += '<h2 class="contact-heading new">'+ 'Contact Details' + '&#58' + '</h2>';
         html += '<div class="contact-block">';
        }
           html += '<ul role="list" class="list w-clearfix w-list-unstyled">';                    
           html += '<li class="list-item">';  
           if (value["Public Phone"]) {
           html += '<a href="' + value["Public Phone"] + '" class="contact-links" target="_blank"><img src="https://uploads-ssl.webflow.com/5e5dd661bbf5f1863333707a/61b793a8bfa7cec02e24a2cf_tel_icon.svg" alt="" class="contact-image">' + value["Public Phone"] + '</a>';
           }
           html += '</li>';                  
           html += '<li class="list-item">';
           html += '<ul role="list" class="list w-clearfix w-list-unstyled">';                    
           html += '<li class="list-item">';  
           if (value["Public Phone"]) {
           html += '<a href="' + value["Public Phone"] + '" class="contact-links" target="_blank"><img src="https://uploads-ssl.webflow.com/5e5dd661bbf5f1863333707a/61b793a8bfa7cec02e24a2cf_tel_icon.svg" alt="" class="contact-image">' + value["Public Phone"] + '</a>';
           }
           html += '</li>';                  
           html += '<li class="list-item">';
           
         html += '</div>';
        html += '</div>';
      html += '<div class="contact-button-wrap">';
      html += '<button id="talk" type="button" class="button talk">contact me&#33</button>';
      html += '</div>';
      html += '<div class="links-block">';  
      html += '<ul role="list" class="list w-clearfix w-list-unstyled">';                    
      html += '<li class="list-item">';  
        if (value.Twitter) {
      html += '<a href="' + value.Twitter + '" class="social-block" target="_blank"><img src="https://uploads-ssl.webflow.com/5e5dd661bbf5f1863333707a/5ff60355c347a0de7ee9767f_twitter1.svg" alt="" class="social-icon"></a>';
      }
      html += '</li>';                  
      html += '<li class="list-item">'; 
        if (value.Facebook) {
      html += '<a href="' + value.Facebook + '" class="social-block" target="_blank"><img src="https://uploads-ssl.webflow.com/5e5dd661bbf5f1863333707a/5ff60373c2c2b851a8f9368c_facebook1.svg" alt="" class="social-icon"></a>';
      }
      html += '</li>';                  
      html += '<li class="list-item">'; 
        if (value.Linkedin) {
      html += '<a href="' + value.Linkedin + '" class="social-block" target="_blank"><img src="https://uploads-ssl.webflow.com/5e5dd661bbf5f1863333707a/5ff6038b5e4963165877455b_linkedin1.svg" alt="" class="social-icon"></a>';
      }
      html += '<li class="list-item">'; 
        if (value.Instagram) {
      html += '<a href="' + value.Instagram + '" class="social-block" target="_blank"><img src="https://uploads-ssl.webflow.com/5e5dd661bbf5f1863333707a/615f3768d91e4184e98914bc_instagram1.svg" alt="" class="social-icon"></a>';
      }
      html += '</li>';                      
      html += '</ul>';
      html += '</div>';
      
      html += '</div>';