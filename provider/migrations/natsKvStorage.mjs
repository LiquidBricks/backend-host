// Minimal Umzug storage backed by NATS KV
// Stores an array of executed migration names under a single key.

export class NatsKvStorage {
  constructor({ bucket, key = 'executed' }) {
    this.bucket = bucket
    this.key = key
  }

  async executed() {
    try {
      const entry = await this.bucket.get(this.key)
      if (!entry) return []
      const data = await entry.json()
      return Array.isArray(data) ? data : []
    } catch {
      return []
    }
  }

  async logMigration({ name }) {
    const list = await this.executed()
    if (!list.includes(name)) {
      list.push(name)
      await this.bucket.put(this.key, JSON.stringify(list))
    }
  }

  async unlogMigration({ name }) {
    const list = await this.executed()
    const next = list.filter(n => n !== name)
    await this.bucket.put(this.key, JSON.stringify(next))
  }
}
