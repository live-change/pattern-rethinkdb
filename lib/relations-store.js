const lcp = require("@live-change/pattern")

function relationsStore(r, dbConnection, conn, tableName) {

  const table = r.table(tableName)

  async function createTable() {
    await r.tableCreate(tableName)
    await table.indexCreate('typeAndKeys', [r.row("type"), r.row("keys")])
  }

  async function getRelations(type, keys) {
    let keysList = Object.keys(keys).map(k => [k, keys[k]])
    keysList.sort((a,b) => a[0] == b[0] ? 0 : (a[0] > b[0] ? 1 : -1))
    let keySets = lcp.allCombinations(keysList)
    let promises = new Array(keySets.length)
    for(let i = 0; i < keySets.length; i++) {
      const ks = keySets[i]
      let qObj = {}
      for(let [k,v] of ks) qObj[k] = v
      promises[i] = dbConnection.run(table.getAll([type, keys])).then(cursor => cursor.toArray())
    }
    return (await Promise.all(promises)).reduce((a,b) => a.concat(b), [])
  }

  const relationOperations = new Map() // need to queue operations on keys

  function queueRelationChange(type, keys, relationId, changeFun) {
    const rKey = lcp.relationKey(type, keys)+relationId
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
        const cursor = await dbConnection.run(table.getAll([type, keys]))
        const relations = cursor => cursor.toArray()
        let currentRelation = relations.find(rel => rel.relation == relationId)
        const id = currentRelation.id
        while(op.changes.length > 0) {
          for (const change of op.changes) currentRelation = change(currentRelation)
          op.changes = []
          if (currentRelation) {
            await dbConnection.run(table.get(currentRelation.id).update(currentRelation))
          } else {
            await dbConnection.run(table.get(currentRelation.id).delete())
          }
        }
        relationOperations.delete(rKey)
      }
    })
  }

  async function saveRelation(relation, mark = null) {
    let promises = []
    for(const type of relation.eventTypes) {
      promises.push(
          queueRelationChange(type, relation.keys, relation.relation, (currentRelation) => {
            if(currentRelation) {
              currentRelation.prev.push(...relation.prev)
              if(mark) mark(currentRelation)
              return currentRelation
            } else {
              if(mark) mark(relation)
              return relation
            }
          })
      )
    }
    await Promise.all(promises)
  }

  async function removeRelation(relation) {
    let promises = []
    for(const type of relation.eventTypes) {
      promises.push(
          queueRelationChange(type, relation.keys, relation.relation, (currentRelation) => null)
      )
    }
    await Promise.all(promises)
  }

  return {
    createTable,
    getRelations,
    saveRelation,
    removeRelation
  }

}

module.exports = { relationsStore }