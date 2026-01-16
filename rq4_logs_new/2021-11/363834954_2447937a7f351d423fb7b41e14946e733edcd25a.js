

const CampaignIcons = () => {
   
   const Icons = [

      {
            image: '/images/Campaign-icons/Agriculture.svg',
            title: 'Agriculture'
      },
      {
      image: 'images/Campaign-icons/Community Development.svg',
      title: 'Comunity Development'
      },
      {
      image: 'images/Campaign-icons/Crisis-Peace Intervention.svg',
      title: 'Crisis-Peace Intervention'
      },
      {
      image: 'images/Campaign-icons/Disaster Intervention.svg',
      title: 'Disaster Intervention'
      },
      {
      image: 'images/Campaign-icons/Education.svg',
      title: 'Education'
      },
      {
      image: 'images/Campaign-icons/Elders intervention.svg',
      title: 'Elders intervention'
      },
      {
      image: 'images/Campaign-icons/Girl Child.svg',
      title: 'Girl Child'
      },
      {
      image: 'images/Campaign-icons/Health.svg',
      title: 'Health'
      },
      {
      image: 'images/Campaign-icons/Hospitality.svg',
      title: 'Hospitality'
      },
      {
      image: 'images/Campaign-icons/Less Privilege.svg',
      title: 'Less Privilege'
      },
      {
      image: 'images/Campaign-icons/Politics.svg',
      title: 'Politics'
      },
      {
      image: 'images/Campaign-icons/Religion.svg',
      title: 'Religion'
      },
      {
      image: 'images/Campaign-icons/Sports Support.svg',
      title: 'Sports Support'
      },
      {
      image: 'images/Campaign-icons/Youth Empowerment.svg',
      title: 'Youth Empowerment'
      }
      



]


   return(
      <section className="Campaign-icons">
         {Icons.map((item, index) => {
            return (
               <div className="icons">
                     <a href="#"><img src={item.image} alt="campaign-icons" /></a>
                     <p>{item.title}</p>
               </div>
            );
         
         })}
         
         
            </section>







   
         );}

export default CampaignIcons