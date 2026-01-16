// data/faqData.ts
export interface FAQItem {
    id: number;
    question: string;
    answer: string;
  }
  
  export const faqData: FAQItem[] = [
    {
      id: 1,
      question: "What is Totally Optimized Solutions?",
      answer: "Totally Optimized Solutions is a leading IT company specializing in custom software development, IT consulting, and digital transformation services. We focus on delivering tailored solutions that drive business success and innovation."
    },
    {
      id: 2,
      question: "What services do you offer?",
      answer: "We offer a comprehensive range of services including custom software development, IT consulting, cloud solutions, mobile app development, and digital transformation. Our goal is to help businesses leverage technology to achieve their objectives."
    },
    {
      id: 3,
      question: "How can I get in touch with you?",
      answer: "You can contact us through our website's contact form, or by sending us an email. We also offer consultation calls to discuss your project needs and how we can assist you."
    },
    {
      id: 4,
      question: "What industries do you specialize in?",
      answer: "We have expertise in various industries including finance, healthcare, e-commerce, and education. Our team is experienced in creating solutions tailored to the specific needs and challenges of different sectors."
    },
    {
      id: 5,
      question: "Can you work with existing systems?",
      answer: "Yes, we can integrate our solutions with your existing systems. Whether you need enhancements, additional features, or data migration, we ensure seamless integration and minimal disruption to your operations."
    },
  ];
  