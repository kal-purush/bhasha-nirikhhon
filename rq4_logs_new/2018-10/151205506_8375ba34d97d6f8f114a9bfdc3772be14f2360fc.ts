import { TrackingCategory, SchemaType, Operations } from './models';

export const definedCategories: TrackingCategory[] = [
  {
    name: 'food',
    columns: [
      { name: 'name', type: SchemaType.TEXT },
      { name: 'calories', type: SchemaType.INT },
      { name: 'timestamp', type: SchemaType.AUTOTIME }
    ]
  },
  {
    name: 'lift',
    columns: [
      { name: 'lift name', type: SchemaType.TEXT },
      { name: 'sets', type: SchemaType.INT },
      { name: 'weight', type: SchemaType.DECIMAL },
      { name: 'notes', type: SchemaType.TEXT },
      { name: 'timestamp', type: SchemaType.AUTOTIME }
    ]
  },
  {
    name: 'run',
    columns: [
      { name: 'distance', type: SchemaType.DISTANCE },
      { name: 'seconds', type: SchemaType.INT },
      { name: 'timestamp', type: SchemaType.AUTOTIME },
      { name: 'pace', type: SchemaType.AUTO_CALCULATED, config: {
          columns: ['distance', 'seconds'],
          operation: Operations.DIVIDE
        }
      }
    ]
  }
];