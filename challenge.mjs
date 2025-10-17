// challenge.mjs

const EVENT_EXPIRY_MS = 2 * 60 * 1000; // 2 minutes
const BLOCKING_TIMEOUT_MS = 30 * 1000; // 30 seconds

// Store all events by key
const events = new Map(); // key -> [{ id, data, timestamp }]
// Track which events each (key, groupId) pair has consumed
const groupConsumption = new Map(); // "key:groupId" -> Set(eventId)
// Track waiting consumers (for blocking get)
const waiters = new Map(); // key -> [resolve]

// Periodically clean expired events
setInterval(() => {
  const now = Date.now();
  for (const [key, evts] of events.entries()) {
    const valid = evts.filter(e => now - e.timestamp < EVENT_EXPIRY_MS);
    if (valid.length > 0) {
      events.set(key, valid);
    } else {
      events.delete(key);
    }
  }
}, 10 * 1000); // cleanup every 10 seconds

/**
 * Push new event into queue for a specific key
 * @param {string} key 
 * @param {object} data 
 */
export async function push(key, data) {
  if (!key) throw new Error("Missing key");

  const newEvent = {
    id: `${key}-${Date.now()}-${Math.random().toString(36).slice(2)}`,
    data: data || {},
    timestamp: Date.now(),
  };

  if (!events.has(key)) events.set(key, []);
  events.get(key).push(newEvent);

  // Wake up waiting consumers (if any)
  if (waiters.has(key)) {
    for (const resolve of waiters.get(key)) resolve();
    waiters.delete(key);
  }
}

/**
 * Return unconsumed events for a given key + groupId.
 * If none available, wait (long-poll) up to 30s for new events.
 * @param {string} key 
 * @param {string} groupId 
 * @returns {Promise<Array>}
 */
export async function blockingGet(key, groupId) {
  if (!key || !groupId) return [];

  const groupKey = `${key}:${groupId}`;

  const getUnconsumed = () => {
    const allEvents = events.get(key) || [];
    const consumed = groupConsumption.get(groupKey) || new Set();
    return allEvents.filter(e => !consumed.has(e.id));
  };

  // Check existing unconsumed events
  let unconsumed = getUnconsumed();
  if (unconsumed.length > 0) {
    markConsumed(groupKey, unconsumed);
    return unconsumed;
  }

  // No events -> wait up to 30 seconds for new event
  await new Promise(resolve => {
    if (!waiters.has(key)) waiters.set(key, []);
    waiters.get(key).push(resolve);
    setTimeout(resolve, BLOCKING_TIMEOUT_MS);
  });

  unconsumed = getUnconsumed();
  if (unconsumed.length > 0) {
    markConsumed(groupKey, unconsumed);
    return unconsumed;
  }

  return []; // timeout → no new event
}

//  mark events as consumed for a group
function markConsumed(groupKey, eventsArr) {
  if (!groupConsumption.has(groupKey))
    groupConsumption.set(groupKey, new Set());
  const consumed = groupConsumption.get(groupKey);
  for (const e of eventsArr) consumed.add(e.id);
}
