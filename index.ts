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
        eventLogTableName: string
        pluginLogTableName: string
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
const IS_CURRENTLY_IMPORTING = 'redshift_impordbyttfbbrezinfregb'
const sanitizeSqlIdentifier = (unquotedIdentifier: string): string => {
    return unquotedIdentifier
}
const logMessage = async (message, config, logToRedshift = false) => {
    if (logToRedshift) {
        const query = `INSERT INTO ${sanitizeSqlIdentifier(config.pluginLogTableName)} (event_at, message) VALUES (GETDATE(), $1)`
        const queryResponse = await executeQuery(query, [message], config)
    }
}

export const jobs: RedshiftImportPlugin['jobs'] = {
    importAndIngestEvents: async (payload, meta) => await importAndIngestEvents(payload as ImportEventsJobPayload, meta)
}


export const setupPlugin: RedshiftImportPlugin['setupPlugin'] = async ({ config, cache, jobs, global, storage }) => {
    await logMessage('setupPlugin', config, true)

    const requiredConfigOptions = ['clusterHost', 'clusterPort', 'dbName', 'dbUsername', 'dbPassword']
    for (const option of requiredConfigOptions) {
        if (!(option in config)) {
            throw new Error(`Required config option ${option} is missing!`)
        }
    }
    if (!config.clusterHost.endsWith('redshift.amazonaws.com')) {
        throw new Error('Cluster host must be a valid AWS Redshift host')
    }


    const initialValue = await storage.get(IS_CURRENTLY_IMPORTING)
    console.log('storage (initial value), ', initialValue)
    if (initialValue === true) {
        console.log('EXIT due to initial value = true')
        return
    }
    await storage.set(IS_CURRENTLY_IMPORTING, true)

    const updatedValue = await storage.get(IS_CURRENTLY_IMPORTING)
    console.log('storage (updatedValue), ', updatedValue)
    console.log('storage before running import :', storage,typeof storage)

    await jobs.importAndIngestEvents({ retriesPerformedSoFar: 0, storage: storage }).runIn(10, 'seconds')

    const endValue = await storage.get(IS_CURRENTLY_IMPORTING)
    console.log('after job run value, ', endValue)
}

export const teardownPlugin: RedshiftImportPlugin['teardownPlugin'] = async ({ global, cache, storage }) => {
    console.log('teardown')
    const beforeTearDown = await cache.get(IS_CURRENTLY_IMPORTING)
    console.log('value before teardown', beforeTearDown)
    await storage.set(IS_CURRENTLY_IMPORTING, false)
    console.log('teardown finished')
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
    const { global, cache, config, jobs } = meta
    const totalRowsResult = await executeQuery(
        `SELECT COUNT(1) FROM ${sanitizeSqlIdentifier(config.tableName)} WHERE NOT EXISTS (SELECT 1 FROM ${sanitizeSqlIdentifier(config.eventLogTableName)} WHERE ${sanitizeSqlIdentifier(config.tableName)}.event_id = ${sanitizeSqlIdentifier(config.eventLogTableName)}.event_id)`,
        [],
        config
    )
    if (!totalRowsResult || totalRowsResult.error || !totalRowsResult.queryResult) {
        throw new Error('Unable to connect to Redshift!')
    }
    global.totalRows = Number(totalRowsResult.queryResult.rows[0].count)
    console.log('Rows to import  :', global.totalRows)

    /*
    // if set to only import historical data, take a "snapshot" of the count
    // on the first run and only import up to that point
    if (config.importMechanism === 'Only import historical data') {
        const totalRowsSnapshot = await storage.get('total_rows_snapshot', null)
        if (!totalRowsSnapshot) {
            await storage.set('total_rows_snapshot', Number(totalRowsResult.queryResult.rows[0].count))
        } else {
            global.totalRows = Number(totalRowsSnapshot)
        }*/


    //const storage = await payload.storage
    //const storageValue = await storage.get(IS_CURRENTLY_IMPORTING)
    //console.log('storage (storageValue in method), ', storageValue)
    //console.log('storage in method:', storage, typeof storage)

    if (payload.retriesPerformedSoFar >= 15) {
        console.error(`Import error: Unable to process rows. Skipped them.`)
        //await storage.set(IS_CURRENTLY_IMPORTING, false)
        await jobs
            .importAndIngestEvents({ ...payload, retriesPerformedSoFar: 0})
            .runIn(10, 'seconds')
        return
    }
    
    if (global.totalRows < 1)  {
        console.log(`No rows to process in ${config.tableName}`)
        // await storage.set(IS_CURRENTLY_IMPORTING, false)
        await jobs
            .importAndIngestEvents({ ...payload, retriesPerformedSoFar: 0})
            .runIn(10, 'seconds')
        return
    }

    const query = `SELECT * FROM ${sanitizeSqlIdentifier(
        config.tableName
    )}
    WHERE NOT EXISTS (
        SELECT 1 FROM ${sanitizeSqlIdentifier(config.eventLogTableName)} 
        WHERE ${sanitizeSqlIdentifier(config.tableName)}.event_id = ${sanitizeSqlIdentifier(config.eventLogTableName)}.event_id
        )
    ORDER BY ${sanitizeSqlIdentifier(config.orderByColumn)}
    LIMIT ${EVENTS_PER_BATCH}`

    const queryResponse = await executeQuery(query, [], config)
    if (!queryResponse ) {
        const nextRetrySeconds = 2 ** payload.retriesPerformedSoFar * 3
        console.log('A')
        console.log(
            `Unable to process rows. Retrying in ${nextRetrySeconds}. Error: ${queryResponse.error}`
        )
        await jobs
            .importAndIngestEvents({ ...payload, retriesPerformedSoFar: payload.retriesPerformedSoFar + 1 })
            .runIn(nextRetrySeconds, 'seconds')
        return 
    }
    if (queryResponse.error ) {
        const nextRetrySeconds = 2 ** payload.retriesPerformedSoFar * 3
        console.log('B')
        console.log(
            `Unable to process rows. Retrying in ${nextRetrySeconds}. Error: ${queryResponse.error}`
        )
        await jobs
            .importAndIngestEvents({ ...payload, retriesPerformedSoFar: payload.retriesPerformedSoFar + 1 })
            .runIn(nextRetrySeconds, 'seconds')
        return
    }
    if (!queryResponse.queryResult ) {
        const nextRetrySeconds = 2 ** payload.retriesPerformedSoFar * 3
        console.log('C')
        console.log(
            `Unable to process rows. Retrying in ${nextRetrySeconds}. Error: ${queryResponse.error}`
        )
        await jobs
            .importAndIngestEvents({ ...payload, retriesPerformedSoFar: payload.retriesPerformedSoFar + 1 })
            .runIn(nextRetrySeconds, 'seconds')
        return
    }

    const eventsToIngest: TransformedPluginEvent[] = []

    for (const row of queryResponse.queryResult!.rows) {
        const event = await transformations[config.transformationName].transform(row, meta)
        eventsToIngest.push(event)
    }
    
    const eventIdsIngested = []    

    for (const event of eventsToIngest) {
        posthog.capture(event.event, event.properties)
        eventIdsIngested.push(event.id)
    }
    global.totalRows = global.totalRows - eventIdsIngested.length

    console.log('updated total rows :', global.totalRows)
    
    const joinedEventIds = eventIdsIngested.map(x => `('${x}', GETDATE())`).join(',')

    const insertQuery = `INSERT INTO ${sanitizeSqlIdentifier(
        meta.config.eventLogTableName
    )}
    (event_id, exported_at)
    VALUES
    ${joinedEventIds}`

    const insertQueryResponse = await executeQuery(insertQuery, [], config)
 
    console.log(
        `Processed rows and ingested ${eventsToIngest.length} event${
            eventsToIngest.length > 1 ? 's' : ''
        } from them.`
    )

    if (eventsToIngest.length < EVENTS_PER_BATCH) { // ADAPTED ?
        //await storage.set(IS_CURRENTLY_IMPORTING, false)
        await jobs
            .importAndIngestEvents({ ...payload, retriesPerformedSoFar: 0})
            .runIn(10, 'seconds') 
        return
    }

    
    await jobs.importAndIngestEvents({ retriesPerformedSoFar: 0 }).runNow()
}

const transformations: TransformationsMap = {
    'default': {
        author: 'yakkomajuri',
        transform: async (row, _) => {
            const { event_id, event_at, distinct_id, event, properties, set} = row
            console.log('properties :', properties)
            let eventToIngest = {
                "event": event,
                id:event_id,
                properties: {
                    distinct_id,
                    event_at,
                    ...JSON.parse(properties)
                }
            }
            if (set){
                eventToIngest['properties']['$set'] = JSON.parse(set) 
            }
            return eventToIngest
        }
    }
}
