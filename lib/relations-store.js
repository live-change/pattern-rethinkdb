const lcp = require("@live-change/pattern")

function relationsStore(r, dbConnection, tableName) {

  const table = r.table(tableName)

  async function createTable() {
    await dbConnection.run( r.tableCreate(tableName) )
    await dbConnection.run( table.indexCreate('eventTypeAndKeys', [r.row("eventType"), r.row("keys")]) )
    await dbConnection.run( table.indexCreate('sourceRelation', [r.row("source"), r.row("relation")]) )
  }

  async function getRelations(type, keys) {
    let keysList = Object.keys(keys).map(k => [k, keys[k]]).filter(([a,b]) => !!b)
    keysList.sort((a,b) => a[0] == b[0] ? 0 : (a[0] > b[0] ? 1 : -1))
    let keySets = lcp.allCombinations(keysList)
    let promises = new Array(keySets.length)
    for(let i = 0; i < keySets.length; i++) {
      const ks = keySets[i]
      /*let qObj = {}
      for(let [k,v] of ks) qObj[k] = v*/
      const req = table.getAll([type, ks], { index: "eventTypeAndKeys" })
      promises[i] = dbConnection.run(req).then(cursor => cursor.toArray())
    }
    const results = (await Promise.all(promises)).reduce((a,b) => a.concat(b), [])
    return results
  }

  const relationOperations = new Map() // need to queue operations on keys

  function queueRelationChange(type, keysList, relationId, changeFun) {
    const rKey = JSON.stringify([type, keysList, relationId])
    let op = relationOperations.get(rKey)
    return new Promise(async (resolve, reject) => {
      if(op) {
        op.onDone.push(resolve)
        op.onError.push(reject)
      } else {
        op = {
          changes: [ changeFun ],
          onDone: [ resolve ],
          onError: [ reject ]
        }
        relationOperations.set(rKey, op)
        try {
          const cursor = await dbConnection.run(table.getAll([type, keysList], { index: "eventTypeAndKeys" }))
          const relations = await cursor.toArray()
          let currentRelation = relations.find(rel => rel.relation == relationId)
          while (op.changes.length > 0) {
            for (const change of op.changes) currentRelation = change(currentRelation)
            op.changes = []
            if (currentRelation) {
              await dbConnection.run(table.insert(currentRelation, { conflict: 'update' }))
            } else {
              await dbConnection.run(table.get(currentRelation.id).delete())
            }
          }
          relationOperations.delete(rKey)
          for (const cb of op.onDone) cb('ok')
        } catch(err) {
          for(const cb of op.onError) cb(err)
        }
      }
    })
  }

  async function saveRelation(relation, mark = null) {
    let promises = []
    let keysList = Object.keys(relation.keys).map(k => [k, relation.keys[k]]).filter(([a,b]) => !!b)
    keysList.sort((a,b) => a[0] == b[0] ? 0 : (a[0] > b[0] ? 1 : -1))
    for(const type of relation.eventTypes) {
      promises.push(
          queueRelationChange(type, keysList, relation.relation, (currentRelation) => {
            if(currentRelation) {
              currentRelation.prev.push(...relation.prev)
              if(mark) mark(currentRelation)
              return currentRelation
            } else {
              const r = { ...relation, eventType: type, keys: keysList }
              if(mark) mark(r)
              return r
            }
          })
      )
    }
    await Promise.all(promises)
  }

  async function removeRelation(relation) {
    return dbConnection.run(
        table.getAll([relation.source, relation.relation], { index: "sourceRelation" }).delete()
    )
  }

  return {
    createTable,
    getRelations,
    saveRelation,
    removeRelation
  }

}

module.exports = { relationsStore }