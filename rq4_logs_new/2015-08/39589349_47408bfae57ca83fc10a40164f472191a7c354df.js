CurrentPlays = new orion.collection('current_plays', {
  singularName: 'obra',
  pluralName: 'obras',
  title: 'Obras en Cartelera',
  link: {
    title: 'Obras en Cartelera'
  },
  tabular: {
    columns: [
      { data: 'name', title: 'Nombre' },
      orion.attributeColumn('image', 'image', 'Imagen')
    ]
  }
});

CurrentPlays.attachSchema(new SimpleSchema({
  name: {
    type: String,
    label: 'Nombre'
  },
  image: orion.attribute('image', {
    label: 'Imagen'
  }),
  description: orion.attribute('froala', {
    label: 'Descripción'
  }),
  ticketsLink: {
    type: String,
    label: 'Tickets Link',
    regEx: SimpleSchema.RegEx.Url
  },
  createdAt: orion.attribute('createdAt')
}))