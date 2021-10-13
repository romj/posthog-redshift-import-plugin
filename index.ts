import { Plugin, PluginEvent, PluginMeta } from '@posthog/plugin-scaffold'
import { Client, QueryResult, QueryResultRow } from 'pg'
declare namespace posthog {
    function capture(event: string, properties?: Record<string, any>): void
}
type RedshiftImportPlugin = Plugin<{
    global: {
        pgClient: Client
        eventsToIgnore: Set<string>
        sanitizedTableName: string
        initialOffset: number
        totalRows: number
    }
    config: {
        clusterHost: string
        clusterPort: string
        dbName: string
        tableName: string
        dbUsername: string
        dbPassword: string
        eventsToIgnore: string
        orderByColumn: string
        transformationName: string
        importMechanism: 'Import continuously' | 'Only import historical data'
    }
}>

interface ImportEventsJobPayload extends Record<string, any> {
    offset?: number
    retriesPerformedSoFar: number
}
interface ExecuteQueryResponse {
    error: Error | null
    queryResult: QueryResult<any> | null
}
interface TransformedPluginEvent {
    event: string,
    properties?: PluginEvent['properties']
}
interface TransformationsMap {
    [key: string]: {
        author: string
        transform: (row: QueryResultRow, meta: PluginMeta<RedshiftImportPlugin>) => Promise<TransformedPluginEvent>
    }
}
const EVENTS_PER_BATCH = 10
const REDIS_OFFSET_KEY = 'import_offset_bis'
const sanitizeSqlIdentifier = (unquotedIdentifier: string): string => {
    return unquotedIdentifier
}
export const jobs: RedshiftImportPlugin['jobs'] = {
    importAndIngestEvents: async (payload, meta) => await importAndIngestEvents(payload as ImportEventsJobPayload, meta)
}

console.log('test : random log (line 55)')

export const setupPlugin: RedshiftImportPlugin['setupPlugin'] = async ({ config, cache, jobs, global, storage }) => {
    console.log('setupPlugin blablah')
    const requiredConfigOptions = ['clusterHost', 'clusterPort', 'dbName', 'dbUsername', 'dbPassword']
    for (const option of requiredConfigOptions) {
        if (!(option in config)) {
            throw new Error(`Required config option ${option} is missing!`)
        }
    }
    if (!config.clusterHost.endsWith('redshift.amazonaws.com')) {
        throw new Error('Cluster host must be a valid AWS Redshift host')
    }
    console.log('redshift check OK blablah')
    // the way this is done means we'll continuously import as the table grows
    // to only import historical data, we should set a totalRows value in storage once
    const totalRowsResult = await executeQuery(
        `SELECT COUNT(1) FROM ${sanitizeSqlIdentifier(config.tableName)} WHERE NOT EXISTS (SELECT 1 FROM ${sanitizeSqlIdentifier(config.logTableName)} WHERE ${sanitizeSqlIdentifier(config.tableName)}.event_id = ${sanitizeSqlIdentifier(config.logTableName)}.event_id)`,
        [],
        config
    )
    console.log('totalRowsResult (continuous importation) TER:', totalRowsResult)

    if (!totalRowsResult || totalRowsResult.error || !totalRowsResult.queryResult) {
        throw new Error('Unable to connect to Redshift!')
    }
    global.totalRows = Number(totalRowsResult.queryResult.rows[0].count)

    console.log('5 - global.totalRows  :', global.totalRows)

    // if set to only import historical data, take a "snapshot" of the count
    // on the first run and only import up to that point
    if (config.importMechanism === 'Only import historical data') {
        const totalRowsSnapshot = await storage.get('total_rows_snapshot', null)
        if (!totalRowsSnapshot) {
            await storage.set('total_rows_snapshot', Number(totalRowsResult.queryResult.rows[0].count))
        } else {
            global.totalRows = Number(totalRowsSnapshot)
            console.log('5 - global.totalRows (historical) : ', global.totalRows)
        }
    }

    // used for picking up where we left off after a restart
    const offset = await storage.get(REDIS_OFFSET_KEY, 0)
    console.log('offset :', offset)
    // needed to prevent race conditions around offsets leading to events ingested twice
    global.initialOffset = Number(offset)
    console.log('global.initialOffset : ', global.initialOffset)
    await cache.set(REDIS_OFFSET_KEY, Number(offset) / EVENTS_PER_BATCH)

    //offset : works --> number of new line
    //global
    console.log('5 - offset : ', offset)
    console.log('5 - global.initialOffset : ', global.initialOffset)
    console.log('5 - cache.set :', cache.set)

    await jobs.importAndIngestEvents({ retriesPerformedSoFar: 0 }).runIn(10, 'seconds')
}

const getTotalRowsToImport = async (config) => {
    console.log('const dedicated to count of number of row to import')
    const tableName = sanitizeSqlIdentifier(config.tableName),
          logTableName = sanitizeSqlIdentifier(config.logTableName)
    const totalRowsResultBis = await executeQuery(
        `SELECT COUNT(1) FROM ${tableName} WHERE NOT EXISTS (SELECT 1 FROM ${logTableName} WHERE ${tableName}.event_id = ${logTableName}.event_id)`,
        [],
        config
    )
    console.log('output : ', totalRowsResultBis.queryResult.rows[0].count)
    return Number(totalRowsResultBis.queryResult.rows[0].count)
}

console.log('5 : ', getTotalRowsToImport)

export const teardownPlugin: RedshiftImportPlugin['teardownPlugin'] = async ({ global, cache, storage }) => {
    const redisOffset = await cache.get(REDIS_OFFSET_KEY, 0)
    console.log('redisOffset :', redisOffset)
    const workerOffset = Number(redisOffset) * EVENTS_PER_BATCH
    console.log('workerOffset :', workerOffset)
    const offsetToStore = workerOffset > global.totalRows ? global.totalRows : workerOffset
    console.log('offsetToStore :', offsetToStore)
    await storage.set(REDIS_OFFSET_KEY, offsetToStore)
}
// all the above log about offset are not triggered when historical importation 

//EXECUTE QUERY FUNCTION
const executeQuery = async (
    query: string,
    values: any[],
    config: PluginMeta<RedshiftImportPlugin>['config']
): Promise<ExecuteQueryResponse> => {
    const pgClient = new Client({
        user: config.dbUsername,
        password: config.dbPassword,
        host: config.clusterHost,
        database: config.dbName,
        port: parseInt(config.clusterPort),
    })
    await pgClient.connect()
    let error: Error | null = null
    let queryResult: QueryResult<any> | null = null
    try {
        queryResult = await pgClient.query(query, values)
    } catch (err) {
        error = err
    }
    await pgClient.end()
    return { error, queryResult }
}


const importAndIngestEvents = async (
    payload: ImportEventsJobPayload,
    meta: PluginMeta<RedshiftImportPlugin>
) => {
    if (payload.offset && payload.retriesPerformedSoFar >= 15) {
        console.error(`Import error: Unable to process rows ${payload.offset}-${
            payload.offset + EVENTS_PER_BATCH
        }. Skipped them.`)
        return
    }
    for (let key in payload) {
        console.log('5 (keys) - ', key);
    }

    console.log('5 - meta[global, cache, config, jobs] : ', meta.global, meta.cache, meta.config, meta.jobs)
    const { global, cache, config, jobs } = meta
    console.log('5 - meta[global, cache, config, jobs] (after attribution) : ', meta.global, meta.cache, meta.config, meta.jobs)

    let offset: number
    if (payload.offset) {
        console.log('5 - first condition of payload : ', payload.offset)
        offset = payload.offset
    } else {
        const redisIncrementedOffset = await cache.incr(REDIS_OFFSET_KEY)
        offset = global.initialOffset + (redisIncrementedOffset - 1) * EVENTS_PER_BATCH
        console.log('5 - 2nd condition of payload : redisIncremented : ', redisIncrementedOffset, 'offset : ', offset)
    }
    console.log('5 - offset, global.totalRows : ', offset, global.totalRows)
    if (offset > global.totalRows) {
        console.log(`Done processing all rows in ${config.tableName}`)
        return
    }

    const query = `SELECT * FROM ${sanitizeSqlIdentifier(
        meta.config.tableName
    )}
    ORDER BY ${sanitizeSqlIdentifier( config.orderByColumn)}
    OFFSET $1 LIMIT ${EVENTS_PER_BATCH}`
    const values = [offset]
    console.log('5 - values : ', values)
    const queryResponse = await executeQuery(query, values, config)
    if (!queryResponse || queryResponse.error || !queryResponse.queryResult) {
        const nextRetrySeconds = 2 ** payload.retriesPerformedSoFar * 3
        console.log(
            `Unable to process rows ${offset}-${
                offset + EVENTS_PER_BATCH
            }. Retrying in ${nextRetrySeconds}. Error: ${queryResponse.error}`
        )
        await jobs
            .importAndIngestEvents({ ...payload, retriesPerformedSoFar: payload.retriesPerformedSoFar + 1 })
            .runIn(nextRetrySeconds, 'seconds')
    }
    const eventsToIngest: TransformedPluginEvent[] = []
    for (const row of queryResponse.queryResult!.rows) {
        const event = await transformations[config.transformationName].transform(row, meta)
        eventsToIngest.push(event)
    }
    for (const event of eventsToIngest) {
        console.log(event)
        posthog.capture(event.event, event.properties)
    }
    console.log(
        `Processed rows ${offset}-${offset + EVENTS_PER_BATCH} and ingested ${eventsToIngest.length} event${
            eventsToIngest.length > 1 ? 's' : ''
        } from them.`
    )
    await jobs.importAndIngestEvents({ retriesPerformedSoFar: 0 }).runNow()
}


// Transformations can be added by any contributor
// 'author' should be the contributor's GH username
const transformations: TransformationsMap = {
    'default': {
        author: 'yakkomajuri',
        transform: async (row, _) => {
            const { timestamp, distinct_id, event, properties } = row
            const eventToIngest = {
                event,
                properties: {
                    timestamp,
                    distinct_id,
                    ...JSON.parse(properties),
                    source: 'redshift_import',
                }
            }
            return eventToIngest
        }
    },
    'JSON Map': {
        author: 'yakkomajuri',
        transform: async (row, { attachments }) => {
            if (!attachments.rowToEventMap) {
                throw new Error('Row to event mapping JSON file not provided!')
            }

            let rowToEventMap: Record<string, string> = {}
            try {
                rowToEventMap = JSON.parse(attachments.rowToEventMap.contents.toString())
            } catch {
                throw new Error('Row to event mapping JSON file contains invalid JSON!')
            }
            const eventToIngest = {
                event: '',
                properties: {} as Record<string, any>
            }
            for (const [colName, colValue] of Object.entries(row)) {
                if (!rowToEventMap[colName]) {
                    continue
                }
                if (rowToEventMap[colName] === 'event') {
                    eventToIngest.event = colValue
                } else {
                    eventToIngest.properties[rowToEventMap[colName]] = colValue
                }
            }
            return eventToIngest
        }
    }
}

