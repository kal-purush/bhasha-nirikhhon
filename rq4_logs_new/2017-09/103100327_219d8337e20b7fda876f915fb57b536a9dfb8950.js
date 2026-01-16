const models = require('./models')
      fields = Object.keys(models).reduce((res, collection) => {
        res[collection] = Object.keys(models[collection].data[0])
        return res
      }, {})

module.exports = {
  view: collection => {
    checkCollection(collection)

    function* router() {
      return models[collection].view()
    }
    return router()
  },

  remove: collection => {
    checkCollection(collection)
    if (models[collection].data.length < 1)
      throw new Error(`It's empty`)

    function* router() {
      console.log(`Choose id: ${models[collection].get('id')}`)
      const deleted = models[collection].remove(Number(yield 'id'))
      console.log('Element deleted:')
      console.log(deleted)
      yield
    }
    return router()
  },

  insert: collection => {
    checkCollection(collection)

    function* router() {
      const required = fields[collection].filter(i => i !== 'id'),
            item = {}

      for (let i = 0; i < required.length; i++) {
        console.log(`Set ${required[i]}`)
        item[required[i]] = yield 'field'
      }

      let hardwareIndxs
      if (collection === 'computers') {
        const types = models.hardware.get('type')
        hardwareIndxs = []

        for (let i = 0; i < types.length; i++) {
          const variants = models.hardware.get().filter(item => item.type == types[i])
          console.log(`Choose variant for type ${types[i]}:`)
          variants.forEach((item, i) => console.log(`${i + 1}. ${item.name}`))

          const indx = (yield 'indx') - 1
          if (variants[indx]) {
            hardwareIndxs.push(variants[indx].id)
          } else {
            throw new Error ('Wrong index')
          }
        }
      }

      models[collection].insert(item, hardwareIndxs)
      console.log('Element has inserted:')
      console.log(item)
      yield item
    }
    return router()
  },

  update: collection => {
    checkCollection(collection)
    if (models[collection].data.length < 1)
      throw new Error(`It's empty`)

    function* router() {
      console.log(`Choose id: ${models[collection].get('id')}`)

      const id = Number(yield 'id'),
            required = fields[collection].filter(i => i !== 'id'),
            item = {}
      for (let i = 0; i < required.length; i++) {
        console.log(`Set ${required[i]}`)
        item[required[i]] = yield 'field'
      }
      models[collection].update(id, item)
      yield item
    }
    return router()
  },

  filter: () => {
    function* router() {
      return models.computers.data
              .filter(i => i.embedded_adapter)
              .forEach(i => console.log(i))
    }
    return router()
  },

  exit: () => {
    process.exit(0)
  },

  help: function() {
    function* router() {
      console.log('Commands:')
      return Object.keys(this).forEach(command => console.log(` - ${command}`))
    }
    return router.call(this)
  }
}

function checkCollection(collection) {
  if (!models[collection])
    throw new Error(`Wrong collection. Use one of them: ${Object.keys(models)}`)
}