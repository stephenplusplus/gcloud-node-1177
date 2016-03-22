'use strict'

const async = require('async')
const gcloud = require('gcloud')
const request = require('request')
const uuid = require('node-uuid')

const bigquery = gcloud.bigquery()
const gcs = gcloud.storage()

const TESTS_PREFIX = 'gcloud_test_'
const getId = () => {
  return TESTS_PREFIX + uuid.v1().replace(/-/g, '_')
}

const bucket = gcs.bucket(getId())
const dataset = bigquery.dataset(getId())
const table = dataset.table(getId())

const getBucket = (callback) => { bucket.get({ autoCreate: true }, callback) }
const getDataset = (callback) => { dataset.get({ autoCreate: true }, callback) }
const getTable = (callback) => {
  const schema = 'Name,ID'
  table.get({ autoCreate: true, schema: schema }, callback)
}

const onJobComplete = (job, callback) => {
  checkJobStatus()

  function checkJobStatus() {
    job.getMetadata(function(err, apiResponse) {
      if (!err && apiResponse.status.errorResult) {
        err = new Error(apiResponse.status.errorResult.message)
      }

      if (err) return callback(err)
      if (apiResponse.status.state !== 'DONE') return setTimeout(checkJobStatus, 3000)

      callback()
    })
  }
}

const uploadAndImportFile = (file, callback) => {
  bucket.upload('./data.csv', { destination: file }, (err) => {
    if (err) return callback(err)

    table.import(file, { writeDisposition: 'WRITE_TRUNCATE' }, (err, job) => {
      if (err) return callback(err)
      onJobComplete(job, callback)
    })
  })
}

const files = []
var fileLimit = 100
while (fileLimit--) files.push(bucket.file(getId()))

async.series([getBucket, getDataset, getTable], (err) => {
  if (err) throw err

  async.eachLimit(files, 10, uploadAndImportFile, (err) => {
    if (err) throw err

    async.series([
      bucket.deleteFiles.bind(bucket),
      bucket.delete.bind(bucket),
      table.delete.bind(table),
      dataset.delete.bind(dataset)
    ], (err) => {
      if (err) throw err
      console.log('all good')
    })
  })
})
