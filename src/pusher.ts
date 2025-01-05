import axios, {AxiosResponse} from "axios";
import axiosRetry from "axios-retry";
import {IFieldValueMap, IRecord, Vika} from "@vikadata/vika";
// Logger setup
import logger from './logger';
import { exit } from "process";

// Configuration
interface Config {
    aktoolsURL: string;
    vikaToken: string;
    vikaDatasheetId: string;
    vikaDatasheetIdExRate: string;
    vikaViewId: string;
    coingeckoApiKey: string;
    apiTimeout: number;
}

const CONFIG: Config = {
    aktoolsURL: "http://127.0.0.1:8080", // todo:
    vikaToken: "", // Your Vika token
    vikaDatasheetId: "dstxGhrxCvre4TKp36",
    vikaDatasheetIdExRate: "dstNlVoLWMJeDez8rS",
    vikaViewId: "viwyDAMb2JjVD",
    coingeckoApiKey: "CG-u6fW2ohCTveS1FrqUVcNawYo",
    apiTimeout: 10000, // 10s
};

interface CacheObject<T> {
    data: T | null;
    timestamp: number | null;
    isValid: () => boolean;
}

function createCache<T>(cacheDuration: number = 60 * 60 * 1000): CacheObject<T> {
    return {
        data: null,
        timestamp: null,
        isValid: function () {
            return this.data !== null && (Date.now() - (this.timestamp ?? 0) < cacheDuration);
        }
    };
}

// Cache
const stockDataCache = createCache<any[]>();
const fundEtfCategorySinaETFCache = createCache<any[]>();
const fundEtfCategorySinaLOFCache = createCache<any[]>();
const fundEtfCategorySinaFBSCache = createCache<any[]>();
const fundEtfSpotEmCache = createCache<any[]>();
const fundLofSpotEmCache = createCache<any[]>();

// const customDurationCache = createCache<any[]>(30 * 60 * 1000); // 30mins

// Configure axios to use axios-retry
axiosRetry(axios, {retries: 3, retryDelay: axiosRetry.exponentialDelay});

// API functions
interface API {
    fetchDanjuanFundNav(code: string): Promise<number>

    fxSpotQuote(code: string): Promise<number>;

    coingeckoSimplePrice(id: string): Promise<number>;

    stockHkHistMinEm(code: string): Promise<number>;

    stockUsHistMinEm(code: string): Promise<number>;

    stockUsIexapis(code: string): Promise<number>;

    stockZhASpotEm(code: string): Promise<number>;

    fundOpenFundInfoEm(code: string): Promise<number>;

    fundEtfCategorySinaETF(code: string): Promise<number>;

    fundEtfCategorySinaLOF(code: string): Promise<number>;

    fundEtfCategorySinaFBS(code: string): Promise<number>;

    fundEtfSpotEm(code: string): Promise<number>;

    fundLofSpotEm(code: string): Promise<number>;
}

interface DanjuanResponse {
    data: {
        items: Array<{
            date: string;
            nav: string;
            percentage: string;
            value: string;
        }>;
    };
    result_code: number;
}

const api: API = {

    async fetchDanjuanFundNav(code: string): Promise<number> {
        try {
            const res: AxiosResponse<DanjuanResponse> = await utils.withTimeout(
                axios.get(
                    `https://danjuanfunds.com/djapi/fund/nav/history/${code}?page=1&size=1`
                ),
                CONFIG.apiTimeout
            );

            if (res.data.result_code === 0 && res.data.data.items.length > 0) {
                const navValue = parseFloat(res.data.data.items[0].nav);
                if (!isNaN(navValue) && navValue > 0) {
                    return navValue;
                }
            }

            logger.debug("Danjuan fund NAV data not found or invalid", { code });
            return -1;
        } catch (error) {
            logger.debug("Error fetching Danjuan fund NAV data", {
                code,
                error: (error as Error).message,
            });
            return -1;
        }
    },

    async fxSpotQuote(code: string): Promise<number> {
        try {
            const res: AxiosResponse = await utils.withTimeout(
                axios.get(`${CONFIG.aktoolsURL}/api/public/fx_spot_quote`),
                CONFIG.apiTimeout
            );
            const item = res.data.find((item: any) => item["货币对"] === code);
            const value = item ? item["买报价"] : -1;
            if (value === -1) {
                logger.warn("FX spot quote not found", {code});
            }
            return value;
        } catch (error) {
            logger.error("Error fetching FX spot quote", {
                code,
                error: (error as Error).message,
            });
            return -1;
        }
    },

    async coingeckoSimplePrice(id: string): Promise<number> {
        const url = `https://api.coingecko.com/api/v3/simple/price?ids=${id}&vs_currencies=usd&x_cg_demo_api_key=${CONFIG.coingeckoApiKey}`;
        try {
            const response: AxiosResponse = await utils.withTimeout(
                axios.get(url, {
                    headers: {
                        authority: "api.coingecko.com",
                        accept:
                            "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                        "accept-language": "zh;q=0.5",
                        "cache-control": "max-age=0",
                        "sec-ch-ua":
                            '"Chromium";v="122", "Not(A:Brand";v="24", "Brave";v="122"',
                        "sec-ch-ua-mobile": "?0",
                        "sec-ch-ua-platform": '"macOS"',
                        "sec-fetch-dest": "document",
                        "sec-fetch-mode": "navigate",
                        "sec-fetch-site": "none",
                        "sec-fetch-user": "?1",
                        "sec-gpc": "1",
                        "upgrade-insecure-requests": "1",
                        "user-agent":
                            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                        Origin: "https://api.coingecko.com",
                        referer: "https://api.coingecko.com/",
                    },
                }),
                CONFIG.apiTimeout
            );
            const value = response.data[id]?.usd ?? -1;
            if (value === -1) {
                logger.debug("Coingecko price not found", {id});
            }
            return value;
        } catch (error) {
            logger.debug("Error fetching Coingecko price", {
                id,
                error: (error as Error).message,
            });
            return -1;
        }
    },

    async stockHkHistMinEm(code: string): Promise<number> {
        try {
            const res: AxiosResponse = await utils.withTimeout(
                axios.get(
                    `${CONFIG.aktoolsURL}/api/public/stock_hk_hist_min_em?symbol=${code}&period=1&adjust=&start_date=2022-01-01`
                ),
                CONFIG.apiTimeout
            );
            const value = res.data[res.data.length - 1]["最新价"] ?? -1;
            if (value === -1) {
                logger.debug("HK stock data not found", {code});
            }
            return value;
        } catch (error) {
            logger.debug("Error fetching HK stock data", {
                code,
                error: (error as Error).message,
            });
            return -1;
        }
    },

    async stockUsHistMinEm(code: string): Promise<number> {
        try {
            const res: AxiosResponse = await utils.withTimeout(
                axios.get(
                    `${CONFIG.aktoolsURL}/api/public/stock_us_hist_min_em?symbol=${code}`
                ),
                CONFIG.apiTimeout
            );
            const value = res.data[res.data.length - 1]["最新价"] ?? -1;
            if (value === -1) {
                logger.debug("US stock data not found", {code});
            }
            return value;
        } catch (error) {
            logger.debug("Error fetching US stock data", {
                code,
                error: (error as Error).message,
            });
            return -1;
        }
    },

    async stockUsIexapis(code: string): Promise<number> {
        try {
            let url = `https://cloud.iexapis.com/stable/stock/${code}/quote?token=pk_048c4e35e0a54a7182d412e18c0dc027`;
            const res: AxiosResponse = await utils.withTimeout(
                axios.get(
                    url,
                    {
                        headers: {
                            referer: "https://fintel.io/",
                            accept: '*/*',
                            'accept-language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
                            dnt: '1',
                            origin: 'https://fintel.io',
                            'priority': 'u=1, i',
                            'sec-ch-ua': '"Chromium";v="127", "Not)A;Brand";v="99"',
                            'sec-ch-ua-mobile': '?0',
                            'sec-ch-ua-platform': '"macOS"',
                            'sec-fetch-dest': 'empty',
                            'sec-fetch-mode': 'cors',
                            'sec-fetch-site': 'cross-site',
                            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
                            // 'Cookie': 'ctoken=27fe30c3f82a404aa28fcbee0c45a957'
                        },
                    }
                ),
                CONFIG.apiTimeout
            );
            const value = res.data["latestPrice"] > 0 ? res.data["latestPrice"] : -1;
            if (value === -1) {
                logger.debug("US stock data from IEX not found", {code});
            }
            return value;
        } catch (error) {
            logger.debug("Error fetching US stock data from IEX", {
                code,
                error: (error as Error).message,
            });
            return -1;
        }
    },

    async stockZhASpotEm(code: string): Promise<number> {
        // If the cache is valid, use the cache directly.
        if (stockDataCache.isValid()) {
            const item = stockDataCache.data!.find(
                (item: any) => item["代码"] === code.split(".")[0]
            );
            return item ? item["最新价"] : -1;
        }

        try {
            // If the cache is invalid, initiate a new request.
            const res: AxiosResponse = await utils.withTimeout(
                axios.get(`${CONFIG.aktoolsURL}/api/public/stock_zh_a_spot_em`),
                CONFIG.apiTimeout
            );

            // update cache
            stockDataCache.data = res.data;
            stockDataCache.timestamp = Date.now();

            const item = res.data.find(
                (item: any) => item["代码"] === code.split(".")[0]
            );
            const value = item ? item["最新价"] : -1;

            if (value === -1) {
                logger.debug("A-share stock data not found", {code});
            }

            return value;
        } catch (error) {
            logger.debug("Error fetching A-share stock data", {
                code,
                error: (error as Error).message,
            });
            return -1;
        }
    },

    async fundOpenFundInfoEm(code: string): Promise<number> {
        try {
            const res: AxiosResponse = await utils.withTimeout(
                axios.get(
                    `${CONFIG.aktoolsURL}/api/public/fund_open_fund_info_em?fund=${code}&indicator=单位净值走势`
                ),
                CONFIG.apiTimeout
            );
            const value = res.data[res.data.length - 1]?.["单位净值"] ?? -1;
            if (value === -1) {
                logger.debug("Open fund data not found", {code});
            }
            return value;
        } catch (error) {
            logger.debug("Error fetching open fund data", {
                code,
                error: (error as Error).message,
            });
            return -1;
        }
    },

    async fundEtfCategorySinaETF(code: string): Promise<number> {
        // 如果缓存有效，直接使用缓存
        if (fundEtfCategorySinaETFCache.isValid()) {
            const item = fundEtfCategorySinaETFCache.data!.find(
                (item: any) => item["代码"].includes(code)
            );
            return item ? item["最新价"] : -1;
        }

        try {
            const res: AxiosResponse = await utils.withTimeout(
                axios.get(`${CONFIG.aktoolsURL}/api/public/fund_etf_category_sina?symbol=ETF基金`),
                CONFIG.apiTimeout
            );

            fundEtfCategorySinaETFCache.data = res.data;
            fundEtfCategorySinaETFCache.timestamp = Date.now();

            const item = res.data.find(
                (item: any) => item["代码"].includes(code)
            );
            const value = item ? item["最新价"] : -1;

            if (value === -1) {
                logger.debug("ETF data not found", {code});
            }

            return value;
        } catch (error) {
            logger.debug("Error fetching ETF data", {
                code,
                error: (error as Error).message,
            });
            return -1;
        }
    },

    async fundEtfCategorySinaLOF(code: string): Promise<number> {
        if (fundEtfCategorySinaLOFCache.isValid()) {
            const item = fundEtfCategorySinaLOFCache.data!.find(
                (item: any) => item["代码"].includes(code)
            );
            return item ? item["最新价"] : -1;
        }

        try {
            const res: AxiosResponse = await utils.withTimeout(
                axios.get(`${CONFIG.aktoolsURL}/api/public/fund_etf_category_sina?symbol=LOF基金`),
                CONFIG.apiTimeout
            );

            fundEtfCategorySinaLOFCache.data = res.data;
            fundEtfCategorySinaLOFCache.timestamp = Date.now();

            const item = res.data.find(
                (item: any) => item["代码"].includes(code)
            );
            const value = item ? item["最新价"] : -1;

            if (value === -1) {
                logger.debug("ETF data not found", {code});
            }

            return value;
        } catch (error) {
            logger.debug("Error fetching ETF data", {
                code,
                error: (error as Error).message,
            });
            return -1;
        }
    },

    async fundEtfCategorySinaFBS(code: string): Promise<number> {
        if (fundEtfCategorySinaFBSCache.isValid()) {
            const item = fundEtfCategorySinaFBSCache.data!.find(
                (item: any) => item["代码"].includes(code)
            );
            return item ? item["最新价"] : -1;
        }

        try {
            const res: AxiosResponse = await utils.withTimeout(
                axios.get(`${CONFIG.aktoolsURL}/api/public/fund_etf_category_sina?symbol=封闭式基金`),
                CONFIG.apiTimeout
            );

            fundEtfCategorySinaFBSCache.data = res.data;
            fundEtfCategorySinaFBSCache.timestamp = Date.now();

            const item = res.data.find(
                (item: any) => item["代码"].includes(code)
            );
            const value = item ? item["最新价"] : -1;

            if (value === -1) {
                logger.debug("ETF data not found", {code});
            }

            return value;
        } catch (error) {
            logger.debug("Error fetching ETF data", {
                code,
                error: (error as Error).message,
            });
            return -1;
        }
    },

    async fundEtfSpotEm(code: string): Promise<number> {
        if (fundEtfSpotEmCache.isValid()) {
            const item = fundEtfSpotEmCache.data!.find(
                (item: any) => item["代码"].includes(code)
            );
            return item ? item["最新价"] : -1;
        }

        try {
            const res: AxiosResponse = await utils.withTimeout(
                axios.get(`${CONFIG.aktoolsURL}/api/public/fund_etf_spot_em`),
                CONFIG.apiTimeout
            );

            fundEtfSpotEmCache.data = res.data;
            fundEtfSpotEmCache.timestamp = Date.now();

            const item = res.data.find(
                (item: any) => item["代码"].includes(code)
            );
            const value = item ? item["最新价"] : -1;

            if (value === -1) {
                logger.debug("ETF data not found", {code});
            }

            return value;
        } catch (error) {
            logger.debug("Error fetching ETF data", {
                code,
                error: (error as Error).message,
            });
            return -1;
        }
    },

    async fundLofSpotEm(code: string): Promise<number> {
        if (fundLofSpotEmCache.isValid()) {
            const item = fundLofSpotEmCache.data!.find(
                (item: any) => item["代码"].includes(code)
            );
            return item ? item["最新价"] : -1;
        }

        try {
            const res: AxiosResponse = await utils.withTimeout(
                axios.get(`${CONFIG.aktoolsURL}/api/public/fund_lof_spot_em`),
                CONFIG.apiTimeout
            );

            fundLofSpotEmCache.data = res.data;
            fundLofSpotEmCache.timestamp = Date.now();

            const item = res.data.find(
                (item: any) => item["代码"].includes(code)
            );
            const value = item ? item["最新价"] : -1;

            if (value === -1) {
                logger.debug("ETF data not found", {code});
            }

            return value;
        } catch (error) {
            logger.debug("Error fetching ETF data", {
                code,
                error: (error as Error).message,
            });
            return -1;
        }
    },
};

// Utility functions
const utils = {
    sleep: (ms: number): Promise<void> =>
        new Promise((resolve) => setTimeout(resolve, ms)),

    chunk: <T>(array: T[], size: number): T[][] => {
        const chunked: T[][] = [];
        for (let i = 0; i < array.length; i += size) {
            chunked.push(array.slice(i, i + size));
        }
        return chunked;
    },

    withTimeout: <T>(promise: Promise<T>, ms: number): Promise<T> => {
        const timeout = new Promise<never>((_, reject) => {
            setTimeout(
                () => reject(new Error(`Operation timed out after ${ms} ms`)),
                ms
            );
        });
        return Promise.race([promise, timeout]);
    },
};

// Updated Types for Vika data
interface VikaRecord extends IRecord {
    fields: IFieldValueMap;
}

type VikaDatasheet = ReturnType<typeof Vika.prototype.datasheet>;

// Global variable for Vika connector
let vikaConnector: VikaDatasheet | null = null;
let vikaConnectorExcRate: VikaDatasheet | null = null;

// Component functions
interface ComponentFunctions {
    vika_connector: (data: {
        token: string;
        datasheetId: string;
        datesheetIdExcRate: string;
    }) => Promise<VikaDatasheet>;
    vika_queryAll: (vikaConnector: VikaDatasheet | null) => Promise<VikaRecord[]>;
    finance_query: (inputs: { query: VikaRecord[]; record: VikaRecord[]}) => Promise<{result:IRecord[]; resultExcRate:IRecord[]}>;
    vika_update: (inputs: { vikaConnector: VikaDatasheet | null, update: IRecord[] }) => Promise<string>;
    vika_end: () => Promise<void>;
}

const componentFunctions: ComponentFunctions = {
    vika_connector: async function (data: {
        token: string;
        datasheetId: string;
        datesheetIdExcRate: string;
    }) {
        const vika = new Vika({token: data.token, fieldKey: "name"});
        const datasheet = vika.datasheet(data.datasheetId);
        vikaConnector = datasheet;

        const datesheetIdExcRate = vika.datasheet(data.datesheetIdExcRate);
        vikaConnectorExcRate = datesheetIdExcRate;
        return datasheet;
    },

    vika_queryAll: async function (vikaConnector: VikaDatasheet | null) {
        if (!vikaConnector) {
            throw new Error("Vika connector not found");
        }
        const allRecordsIter = vikaConnector.records.queryAll();
        let records: VikaRecord[] = [];
        for await (const eachPageRecords of allRecordsIter) {
            records = records.concat(eachPageRecords as VikaRecord[]);
        }
        return records;
    },

    finance_query: async function (inputs: { query: VikaRecord[], record: VikaRecord[] }) {
        const resultExcRate: IRecord[] = [];
        const result: IRecord[] = [];
        // todo: 要增加更多发币的汇率
        const [USDCNH, HKDCNH] = await Promise.all([
            api.fxSpotQuote("USD/CNY"),
            api.fxSpotQuote("HKD/CNY"),
        ]);

        const [THBCNY, EURCNY, KRWCNY] = await Promise.all([
            api.fxSpotQuote("CNY/THB"),
            api.fxSpotQuote("EUR/CNY"),
            api.fxSpotQuote("CNY/KRW")
        ]);

        const exchangeRates: { [key: string]: number } = {
            "人民币": 1,
            "美元": parseFloat(USDCNH.toFixed(3)),
            "港币": parseFloat(HKDCNH.toFixed(3)),
            "泰铢": parseFloat((1.0 / THBCNY).toFixed(3)),
            "欧元": parseFloat(EURCNY.toFixed(3)),
            "韩币": parseFloat((1.0 / KRWCNY).toFixed(5)),
            // todo: 要增加更多需要支持的汇率
        };

        if (exchangeRates["美元"] === -1 || exchangeRates["港币"] === -1
            || exchangeRates["泰铢"] === -1 || exchangeRates["欧元"] === -1
            || exchangeRates["韩币"] === -1
        ) {
            logger.error("Failed to fetch exchange rates");
            return {result, resultExcRate};
        }

        for (const item of inputs.record) {
            const key = item.fields!["标题"] as string;
            resultExcRate.push({
                recordId: item.recordId,
                fields: {
                    // ...item.fields,
                    '汇率（对人民币）': exchangeRates[key],
                },
            });
        }

        for (const item of inputs.query) {
            const {code, Type, exchange_name} = item.fields as {
                code?: string;
                Type?: string;
                exchange_name?: string;
            };
            if (!code || !Type || !exchange_name) {
                logger.warn("Missing required fields", {
                    recordId: item.recordId,
                    Type: Type,
                    code: code,
                    exchange_name: exchange_name,
                });
                continue;
            }

            const new_exchange_price = exchangeRates[exchange_name] || 1;

            await utils.sleep(1000); // Request frequency limit
            let new_price = await getPriceByType(Type, code);

            if (new_price !== -1) {
                result.push({
                    recordId: item.recordId,
                    fields: {
                        // ...item.fields,
                        new_price,
                        new_exchange_price,
                    },
                });
                logger.info("Successfully fetched and updated asset price", {
                    code,
                    Type,
                    new_price,
                    new_exchange_price,
                    processedCount: result.length,
                    totalCount: inputs.query.length,
                });
            } else {
                logger.warn("Failed to fetch asset price", {
                    code,
                    Type,
                    exchange_name,
                    processedCount: result.length,
                    totalCount: inputs.query.length,
                });
            }
        }

        return {result, resultExcRate};
    },

    vika_update: async function (inputs: { vikaConnector: VikaDatasheet | null, update: IRecord[] }) {
        if (!vikaConnector) {
            throw new Error("Vika connector not found");
        }
        const chunkedRecords = utils.chunk(inputs.update, 10);
        for (const chunk of chunkedRecords) {
            await inputs.vikaConnector!.records.update(chunk);
        }
        return "Update successful";
    },

    vika_end: async function () {
        logger.info("Execution successful", {time: new Date().getTime()});
    },
};

async function getPriceByType(type: string, code: string): Promise<number> {
    switch (type) {
        case "加密货币":
            await utils.sleep(3000);
            return await api.coingeckoSimplePrice(code);
        case "港股股票":
            return await api.stockHkHistMinEm(code);
        // fixme:
        case "美股股票": {
            const histData = await api.stockUsIexapis(code.split(".").pop() ?? "");
            if (histData !== -1) {
                return histData;
            }
            return await api.stockUsHistMinEm(code);
        }
        case "A股股票": {
            return api.stockZhASpotEm(code);
        }
        case "基金ETF": {
            let histData = await api.fundEtfCategorySinaETF(code);
            if (histData !== -1) {
                return histData;
            }
            histData = await api.fundEtfCategorySinaLOF(code);
            if (histData !== -1) {
                return histData;
            }
            histData = await api.fundEtfCategorySinaFBS(code);
            if (histData !== -1) {
                return histData;
            }
            histData = await api.fundEtfSpotEm(code);
            if (histData !== -1) {
                return histData;
            }
            histData = await api.fundLofSpotEm(code);
            if (histData !== -1) {
                return histData;
            }
            histData = await api.fundOpenFundInfoEm(code);
            if (histData !== -1) {
                return histData;
            }
            return api.fetchDanjuanFundNav(code);
        }
        default:
            return -1;
    }
}

// Error handling and logging
process.on("unhandledRejection", (reason: any, promise: Promise<any>) => {
    logger.error("Unhandled Rejection at:", promise, "reason:", reason);
});

process.on("uncaughtException", (error: Error) => {
    logger.error("Uncaught Exception:", error);
});

// Main execution function
async function main() {
    try {
        // 1. Connect to Vika
        await componentFunctions.vika_connector({
            token: CONFIG.vikaToken,
            datasheetId: CONFIG.vikaDatasheetId,
            datesheetIdExcRate: CONFIG.vikaDatasheetIdExRate,
        });

        // 2. Query all records
        const records = await componentFunctions.vika_queryAll(vikaConnector);
        const recordsExcRate = await componentFunctions.vika_queryAll(vikaConnectorExcRate);

        // 3. Process records and fetch prices
        const {result, resultExcRate} = await componentFunctions.finance_query({
            query: records,
            record: recordsExcRate
        });

        // 4. Update records in Vika
        await componentFunctions.vika_update({vikaConnector, update: result});
        await componentFunctions.vika_update({vikaConnector: vikaConnectorExcRate, update: resultExcRate});

        // 5. End execution
        await componentFunctions.vika_end();
    } catch (error) {
        logger.error("Execution failed", {error: (error as Error).message});
    }
}

// Start the execution
(async () => {
    try {
        await main();
    } catch (error) {
        console.error(error);
    }
})();
