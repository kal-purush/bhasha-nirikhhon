// Static params for product detail pages
export const locales = ['en', 'fr', 'ar'];
export const productIds = ['1', '2', '3', '4', '5', '6'];

export function generateStaticParams() {
  return locales.flatMap(locale => 
    productIds.map(id => ({
      locale,
      id
    }))
  );
} 