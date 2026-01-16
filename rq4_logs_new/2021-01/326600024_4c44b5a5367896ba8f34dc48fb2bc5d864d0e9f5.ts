enum VendorType {
  Seamless = 'SEAMLESS',
  Transfer = 'TRANSFER'
};

type Vendor = {
  _id: string;
  name: string;
  type: VendorType;
  dateTimeCreated: Date;
  dateTimeUpdated: Date; 
}

const entity = () => {
  return async function vendor({
    _id,
    name,
    type,
    dateTimeCreated,
    dateTimeUpdated,
  }) {
    if (!_id) {
      throw new Error(`Please input ID`);
    }

    if (!name) {
      throw new Error(`Please input name`);
    }

    if (!type) {
      throw new Error(`Please input type`);
    }

    if (type !== 'SEAMLESS' && type !== 'TRANSFER') {
      throw new Error(`Invalid type`);
    }

    const finalVendor: Vendor = {
      name,
      type,
      _id,
      dateTimeCreated,
      dateTimeUpdated,
    }

    return finalVendor
  };
};

export default entity;