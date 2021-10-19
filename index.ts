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

const EVENTS_PER_BATCH = 500
const IS_CURRENTLY_IMPORTING = 'stripped_import_plugin'
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
    console.log('config', config)
    console.log('cache', cache)
    console.log('job', jobs)
    console.log('global', global)
    console.log('storage', storage)

    const initialValue = await storage.get(IS_CURRENTLY_IMPORTING)
    
    await logMessage(`initialValue = ${initialValue}`, config, true)
    
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
    await storage.set(IS_CURRENTLY_IMPORTING, false)
    console.log('done tearing down')
}

const importAndIngestEvents = async (
    payload: ImportEventsJobPayload,
    meta: PluginMeta<RedshiftImportPlugin>
) => {
    const { global, cache, config, jobs } = meta
    logMessage('importAndIngestEvents', config, true)
    console.log('importAndIngestEvents - config', config)
    console.log('importAndIngestEvents - cache', cache)
    console.log('importAndIngestEvents - job', jobs)
    console.log('importAndIngestEvents - global', global)
    return 
}
