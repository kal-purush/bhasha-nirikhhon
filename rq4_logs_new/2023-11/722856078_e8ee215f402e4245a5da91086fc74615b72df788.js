import UrlParser from '../../routes/url-route';
import createFaunaDetailTemplate from '../templates/detail-page-template';

const DetailPage = {
    async fetchFaunaData() {
        try {
          const response = await fetch('/data/data-fauna.json');
          if (!response.ok) {
            throw new Error('Failed to fetch data');
          }
          const faunaData = await response.json();
          console.log(faunaData); // Tambahkan log untuk memeriksa hasil fetch
          return faunaData.faunaData;
        } catch (error) {
          console.error('Error fetching fauna data:', error);
          return [];
        }
      },
      

  async render() {
    const url = UrlParser.parseActiveUrlWithoutCombiner();
    const faunaId = url.id;
    
    const faunaData = await this.fetchFaunaData(); // Fetch fauna data

    const faunaDetail = faunaData.find(item => item.nama === faunaId);

    if (!faunaDetail) {
      return '<p>Detail not found</p>';
    }

    const faunaDetailTemplate = createFaunaDetailTemplate(faunaDetail);

    return `
      <div class="container">
        ${faunaDetailTemplate}
      </div>
    `;
  },

  async afterRender() {
    // Any additional logic after rendering
  },
};

export default DetailPage;