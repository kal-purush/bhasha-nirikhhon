import requests
from bs4 import BeautifulSoup

def get_linkedin_profile_details(profile_url):
  """Gets the details of a LinkedIn profile.

  Args:
    profile_url: The URL of the LinkedIn profile.

  Returns:
    A dictionary containing the details of the LinkedIn profile, or None if the
    profile is not found.
  """

  response = requests.get(profile_url)
  if response.status_code != 200:
    return None

   soup = BeautifulSoup(response.content, 'html.parser')

  # Get the basic profile information.
  name = soup.find('h1', class_='text-heading-xlarge inline t-24 v-align-middle break-words').text
  headline = soup.find('h2', class_='text-heading-small inline t-18 v-align-middle break-words').text
  industry = soup.find('p', class_='text-body-small inline t-14 v-align-middle break-words').text
  location = soup.find('span', class_='text-body-small inline t-14 v-align-middle break-words geo-pill').text

  # Get the work experience information.
  work_experience = []
  for work_experience_element in soup.find_all('div', class_='pv-entity__summary-info pv-entity__summary-info--background-section'):
    company_name = work_experience_element.find('h3', class_='pv-entity__company-summary-info__company-name t-16 t-bold')
    job_title = work_experience_element.find('p', class_='pv-entity__summary-info__title t-14 t-black t-normal')
    start_date = work_experience_element.find('span', class_='pv-entity__date-range__start-date t-14 t-black')
    end_date = work_experience_element.find('span', class_='pv-entity__date-range__end-date t-14 t-black')

    work_experience_item = {
      'company_name': company_name.text,
      'job_title': job_title.text,
      'start_date': start_date.text,
      'end_date': end_date.text,
    }

    work_experience.append(work_experience_item)

  # Get the education information.
  education = []
  for education_element in soup.find_all('div', class_='pv-entity__summary-info pv-entity__summary-info--background-section'):
    school_name = education_element.find('h3', class_='pv-entity__company-summary-info__company-name t-16 t-bold')
    degree_name = education_element.find('p', class_='pv-entity__summary-info__title t-14 t-black t-normal')
    start_date = education_element.find('span', class_='pv-entity__date-range__start-date t-14 t-black')
    end_date = education_element.find('span', class_='pv-entity__date-range__end-date t-14 t-black')

    education_item = {
      'school_name': school_name.text,
      'degree_name': degree_name.text,
      'start_date': start_date.text,
      'end_date': end_date.text,
    }

    education.append(education_item)

  # Return the profile details.
  profile_details = {
    'name': name,
    'headline': headline,
    'industry': industry,
    'location': location,
    'work_experience': work_experience,
    'education': education,
  }

  return profile_details


if __name__ == '__main__':
  # Enter the URL of the LinkedIn profile to get the details of.
  profile_url = 'https://www.linkedin.com/in/johndoe'

  # Get the profile details.
  profile_details = get_linkedin_profile_details(profile_url)

  # Print the profile details.
  print(profile_details)