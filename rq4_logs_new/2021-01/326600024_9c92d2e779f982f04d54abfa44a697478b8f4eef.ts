const deleteOneVendor = ({ udeleteOneVendor }: any) => {
    return async function controller(httpRequest: any) {
      const headers = {
        'Content-Type': 'application/json',
      };
  
      try {
        const deleted = await udeleteOneVendor({ id: httpRequest.params.id });
  
        return {
          headers,
          statusCode: 200,
          body: { deleted },
        };
      } catch (e) {
        return {
          headers,
          statusCode: 400,
          body: {
            error: e.message,
          },
        };
      }
    };
  };
  
  export default deleteOneVendor;
  