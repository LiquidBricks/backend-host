import fs from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { MIGRATION_TEMPLATE_PATH } from './run.mjs'

// Generates a reverse-lexicographically sortable 13-digit key based on ms epoch.
// Newer times produce smaller keys so ascending sort yields newest first.
function reverseLexTimeKey(date = new Date()) {
  const now = BigInt(date.getTime())
  // 13-digit max for millisecond epoch (e.g., 9999999999999)
  const MAX_13 = (10n ** 13n) - 1n
  const inverted = MAX_13 - now
  return inverted.toString().padStart(13, '0')
}

function slugify(input) {
  return (input || 'migration')
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    || 'migration'
}

async function ensureDir(dir) {
  await fs.mkdir(dir, { recursive: true })
}

export async function create(title) {
  const __filename = fileURLToPath(import.meta.url)
  const __dirname = path.dirname(__filename)
  const versionsDir = path.resolve(__dirname, 'versions')
  await ensureDir(versionsDir)

  const key = reverseLexTimeKey()
  const name = `${key}-${slugify(title)}.mjs`
  const filePath = path.join(versionsDir, name)

  const template = await fs.readFile(MIGRATION_TEMPLATE_PATH, 'utf8')
  await fs.writeFile(filePath, template, { encoding: 'utf8', flag: 'wx' })
  return filePath
}

// Allow CLI usage: node migrations/create.js "add users table"
if (import.meta.url === `file://${process.argv[1]}`) {
  const title = process.argv.slice(2).join(' ').trim() || 'migration'
  create(title)
    .then((fp) => {
      console.log(`[migrate:create] Created`, fp)
    })
    .catch((err) => {
      if (err && err.code === 'EEXIST') {
        console.error('[migrate:create] A migration with this name already exists.')
      } else {
        console.error('[migrate:create] Error creating migration:', err)
      }
      process.exit(1)
    })
}
