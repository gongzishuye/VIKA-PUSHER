import winston from "winston";

// Get log level from environment variable, default to 'info'
const logLevel = process.env.LOG_LEVEL || 'info';

// Create logger
const logger = winston.createLogger({
    level: logLevel,
    format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.json()
    ),
    transports: [
        new winston.transports.Console({
            format: winston.format.combine(
                winston.format.colorize(),
                winston.format.simple()
            )
        }),
        // Add other transports here, such as file log
        // new winston.transports.File({ filename: 'error.log', level: 'error' }),
        // new winston.transports.File({ filename: 'combined.log' }),
    ]
});

// Export logger
export default logger;