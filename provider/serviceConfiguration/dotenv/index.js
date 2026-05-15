import dotenv from 'dotenv'
import fs from 'fs'

// load base first (lower priority)
if (fs.existsSync('.env')) dotenv.config({ path: '.env' })

// load local second (higher priority)
if (fs.existsSync('.env.local')) dotenv.config({ path: '.env.local', override: true })

export function serviceConfiguration() {
  const { NATS_IP_ADDRESS, PORT } = process.env
  return { NATS_IP_ADDRESS, PORT }
}
