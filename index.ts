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
const EVENTS_PER_BATCH = 500
const IS_CURRENTLY_IMPORTING = 'redshift_impordbyttfbbrezinfregb'
const sanitizeSqlIdentifier = (unquotedIdentifier: string): string => {
    return unquotedIdentifier
}
const logMessage = async (message, config, logToRedshift = false) => {
    console.log(message)
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
    if (initialValue === true) {
        console.log('EXIT due to initial value = true')
        return
    }
    await storage.set(IS_CURRENTLY_IMPORTING, true)

    logMessage('launching job', config, true)
    await jobs.importAndIngestEvents({ retriesPerformedSoFar: 0 }).runIn(10, 'seconds')
    logMessage('finished job', config, true)
   
}

export const teardownPlugin: RedshiftImportPlugin['teardownPlugin'] = async ({ global, cache, storage }) => {
    console.log('teardown')
    const beforeTearDown = await cache.get(IS_CURRENTLY_IMPORTING)
    await storage.set(IS_CURRENTLY_IMPORTING, false)
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
    logMessage('launched', config, true)
}

const transformations: TransformationsMap = {
    'default': {
        author: 'yakkomajuri',
        transform: async (row, _) => {
            const { event_id, timestamp, distinct_id, event, properties, set} = row

            let eventToIngest = {
                "event": event,
                id:event_id,
                properties: {
                    distinct_id,
                    timestamp,
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
